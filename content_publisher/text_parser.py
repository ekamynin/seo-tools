from __future__ import annotations

import re

from bs4 import BeautifulSoup, NavigableString, Tag

from .models import Block, DocumentResult, InlinePart
from .parser import _append_structure_warnings, _classify


_UNORDERED_ITEM_RE = re.compile(r"^\s*[-*•▪◦–—]\s+(.+)$")
_ORDERED_ITEM_RE = re.compile(r"^\s*\d+[.)]\s+(.+)$")
_FONT_SIZE_RE = re.compile(r"font-size\s*:\s*([\d.]+)\s*(pt|px)", re.IGNORECASE)


def _clean_whitespace(text: str) -> str:
    return re.sub(r"\s+", " ", text.replace("\xa0", " "))


def _rich_inline_parts(element: Tag) -> list[InlinePart]:
    raw_parts: list[InlinePart] = []

    def walk(node, inherited_href: str | None = None) -> None:
        if isinstance(node, NavigableString):
            raw_parts.append(InlinePart(str(node), inherited_href))
            return
        if not isinstance(node, Tag) or node.name in ("script", "style", "ul", "ol"):
            return
        href = node.get("href") if node.name == "a" else inherited_href
        for child in node.children:
            walk(child, href)

    walk(element)
    merged: list[InlinePart] = []
    for part in raw_parts:
        cleaned = _clean_whitespace(part.text)
        if not cleaned:
            continue
        if merged and merged[-1].href == part.href:
            merged[-1].text += cleaned
        else:
            merged.append(InlinePart(cleaned, part.href))
    if merged:
        merged[0].text = merged[0].text.lstrip()
        merged[-1].text = merged[-1].text.rstrip()
    return [part for part in merged if part.text]


def _bold_ratio(element: Tag, text_length: int) -> float:
    if not text_length:
        return 0.0
    bold_length = sum(
        len(_clean_whitespace(tag.get_text(" ", strip=True)))
        for tag in element.find_all(("strong", "b"))
    )
    return min(1.0, bold_length / text_length)


def _max_font_size(element: Tag) -> float | None:
    sizes: list[float] = []
    for tag in [element, *element.find_all(True)]:
        classes = set(tag.get("class", []))
        if "ql-size-huge" in classes:
            sizes.append(24.0)
        elif "ql-size-large" in classes:
            sizes.append(18.0)
        elif "ql-size-small" in classes:
            sizes.append(10.0)
        match = _FONT_SIZE_RE.search(tag.get("style", ""))
        if match:
            value = float(match.group(1))
            sizes.append(value if match.group(2).lower() == "pt" else value * 0.75)
    return max(sizes) if sizes else None


def _append_rich_block(
    result: DocumentResult,
    element: Tag,
    role: str = "p",
    explicit_heading_level: int | None = None,
) -> None:
    parts = _rich_inline_parts(element)
    text = "".join(part.text for part in parts).strip()
    if not text:
        return
    result.blocks.append(
        Block(
            index=len(result.blocks) + 1,
            text=text,
            parts=parts,
            role=role,
            explicit_heading_level=explicit_heading_level,
            bold_ratio=_bold_ratio(element, len(text)),
            max_font_size=_max_font_size(element),
        )
    )


def parse_rich_text(html: str, filename: str = "Вставлений текст") -> DocumentResult:
    """Parse semantic HTML captured by the rich-text paste editor."""

    result = DocumentResult(filename=filename)
    soup = BeautifulSoup(html or "", "html.parser")

    def process(element: Tag) -> None:
        name = element.name.lower()
        if name in ("script", "style"):
            return
        if re.fullmatch(r"h[1-6]", name):
            _append_rich_block(
                result,
                element,
                explicit_heading_level=int(name[1]),
            )
            return
        if name in ("ul", "ol"):
            role = name
            for item in element.find_all("li", recursive=False):
                _append_rich_block(result, item, role=role)
                for nested in item.find_all(("ul", "ol"), recursive=False):
                    process(nested)
            return
        if name in ("p", "blockquote", "pre"):
            _append_rich_block(result, element)
            return
        block_children = [
            child
            for child in element.children
            if isinstance(child, Tag)
            and (
                child.name in ("p", "ul", "ol", "blockquote", "pre")
                or re.fullmatch(r"h[1-6]", child.name or "")
            )
        ]
        if block_children:
            for child in block_children:
                process(child)
        else:
            _append_rich_block(result, element)

    for child in soup.children:
        if isinstance(child, Tag):
            process(child)
        elif isinstance(child, NavigableString) and child.strip():
            plain_result = parse_plain_text(str(child), filename)
            result.blocks.extend(plain_result.blocks)

    if not result.blocks:
        result.warnings.append("Не знайдено тексту для конвертації.")

    for index, block in enumerate(result.blocks, start=1):
        block.index = index
    _classify(result.blocks)
    for block in result.blocks:
        if block.role in ("ul", "ol"):
            block.reason = "Список збережено з Google Docs"

    _append_structure_warnings(result)
    uncertain = sum(1 for block in result.blocks if block.confidence < 0.7)
    if uncertain:
        result.warnings.append(
            f"Неоднозначних блоків: {uncertain}. Перевірте їхні типи перед копіюванням."
        )
    return result


def parse_plain_text(text: str, filename: str = "Вставлений текст") -> DocumentResult:
    """Parse pasted plain text into the same semantic blocks as a DOCX."""

    result = DocumentResult(filename=filename)
    normalized = text.replace("\r\n", "\n").replace("\r", "\n")

    for raw_line in normalized.split("\n"):
        line = raw_line.strip()
        if not line:
            continue

        role = "p"
        item_match = _UNORDERED_ITEM_RE.match(line)
        if item_match:
            role = "ul"
            line = item_match.group(1).strip()
        else:
            item_match = _ORDERED_ITEM_RE.match(line)
            if item_match:
                role = "ol"
                line = item_match.group(1).strip()

        if not line:
            continue
        result.blocks.append(
            Block(
                index=len(result.blocks) + 1,
                text=line,
                parts=[InlinePart(line)],
                role=role,
            )
        )

    if not result.blocks:
        result.warnings.append("Не знайдено тексту для конвертації.")

    _classify(result.blocks)
    for block in result.blocks:
        if block.role in ("ul", "ol"):
            block.reason = "Маркер списку у вставленому тексті"

    _append_structure_warnings(result)
    uncertain = sum(1 for block in result.blocks if block.confidence < 0.7)
    if uncertain:
        result.warnings.append(
            f"Неоднозначних блоків: {uncertain}. Перевірте їхні типи перед копіюванням."
        )
    return result
