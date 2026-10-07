from __future__ import annotations

import io
import re
import zipfile
from statistics import median

from docx import Document
from docx.document import Document as _Document
from docx.oxml.ns import qn
from docx.oxml.table import CT_Tbl
from docx.oxml.text.paragraph import CT_P
from docx.table import Table
from docx.text.paragraph import Paragraph

from .models import Block, DocumentResult, InlinePart


_HEADING_NAME_RE = re.compile(r"(?:heading|заголовок)\s*([1-9])", re.IGNORECASE)
_HEADING_ID_RE = re.compile(r"heading([1-9])", re.IGNORECASE)
_TERMINAL_PUNCTUATION_RE = re.compile(r"[.!;:,…]$")
_LONG_SINGLE_BLOCK_THRESHOLD = 300
_LONG_DOCUMENT_THRESHOLD = 500
_MAX_DOCX_ENTRIES = 2_000
_MAX_DOCX_UNCOMPRESSED_SIZE = 50 * 1024 * 1024
_MAX_DOCUMENT_XML_SIZE = 8 * 1024 * 1024
_MAX_COMPRESSION_RATIO = 200


class UnsafeDocxError(ValueError):
    """The uploaded file is not a safe, readable DOCX archive."""


def _validate_docx_archive(data: bytes) -> None:
    """Reject malformed, encrypted, or suspiciously compressed DOCX files."""

    try:
        with zipfile.ZipFile(io.BytesIO(data)) as archive:
            entries = archive.infolist()
            names = {entry.filename for entry in entries}

            if "[Content_Types].xml" not in names or "word/document.xml" not in names:
                raise UnsafeDocxError("Файл не має коректної структури DOCX.")
            if len(entries) > _MAX_DOCX_ENTRIES:
                raise UnsafeDocxError("DOCX містить забагато внутрішніх файлів.")

            total_size = 0
            for entry in entries:
                if entry.flag_bits & 0x1:
                    raise UnsafeDocxError("Захищені паролем DOCX не підтримуються.")
                path_parts = entry.filename.replace("\\", "/").split("/")
                if entry.filename.startswith(("/", "\\")) or ".." in path_parts:
                    raise UnsafeDocxError("DOCX містить небезпечні шляхи файлів.")

                total_size += entry.file_size
                if total_size > _MAX_DOCX_UNCOMPRESSED_SIZE:
                    raise UnsafeDocxError(
                        "DOCX завеликий після розпакування (ліміт 50 МБ)."
                    )
                if (
                    entry.compress_size > 0
                    and entry.file_size / entry.compress_size > _MAX_COMPRESSION_RATIO
                ):
                    raise UnsafeDocxError("DOCX має підозріло високий рівень стиснення.")

            document_xml = archive.getinfo("word/document.xml")
            if document_xml.file_size > _MAX_DOCUMENT_XML_SIZE:
                raise UnsafeDocxError("Текстова частина DOCX перевищує безпечний ліміт.")
    except UnsafeDocxError:
        raise
    except (zipfile.BadZipFile, KeyError, OSError) as exc:
        raise UnsafeDocxError("Файл пошкоджений або не є справжнім DOCX.") from exc


def _iter_document_blocks(document: _Document):
    """Yield paragraphs and tables in their original document order."""

    for child in document.element.body.iterchildren():
        if isinstance(child, CT_P):
            yield Paragraph(child, document)
        elif isinstance(child, CT_Tbl):
            yield Table(child, document)


def _xml_run_text(run_element) -> str:
    chunks: list[str] = []
    for node in run_element.iter():
        if node.tag == qn("w:t"):
            chunks.append(node.text or "")
        elif node.tag == qn("w:tab"):
            chunks.append("\t")
        elif node.tag in (qn("w:br"), qn("w:cr")):
            chunks.append("\n")
    return "".join(chunks)


def _append_part(parts: list[InlinePart], text: str, href: str | None = None) -> None:
    if not text:
        return
    if parts and parts[-1].href == href:
        parts[-1].text += text
    else:
        parts.append(InlinePart(text=text, href=href))


def _extract_parts(paragraph: Paragraph) -> list[InlinePart]:
    """Extract text and regular Word hyperlinks while preserving their order."""

    parts: list[InlinePart] = []
    for child in paragraph._p.iterchildren():
        if child.tag == qn("w:r"):
            _append_part(parts, _xml_run_text(child))
            continue

        if child.tag == qn("w:hyperlink"):
            rel_id = child.get(qn("r:id"))
            anchor = child.get(qn("w:anchor"))
            href: str | None = None
            if rel_id and rel_id in paragraph.part.rels:
                href = paragraph.part.rels[rel_id].target_ref
            elif anchor:
                href = f"#{anchor}"
            for run in child.iter(qn("w:r")):
                _append_part(parts, _xml_run_text(run), href)
            continue

        # Track changes and smart tags may wrap runs one level deeper.
        for run in child.iter(qn("w:r")):
            _append_part(parts, _xml_run_text(run))

    while parts and not parts[0].text.strip():
        parts.pop(0)
    while parts and not parts[-1].text.strip():
        parts.pop()
    if parts:
        parts[0].text = parts[0].text.lstrip()
        parts[-1].text = parts[-1].text.rstrip()
    return [part for part in parts if part.text]


def _run_is_bold(run_element) -> bool:
    r_pr = run_element.find(qn("w:rPr"))
    if r_pr is None:
        return False
    bold = r_pr.find(qn("w:b"))
    if bold is None:
        return False
    value = bold.get(qn("w:val"))
    return value not in ("0", "false", "off")


def _run_font_size(run_element) -> float | None:
    r_pr = run_element.find(qn("w:rPr"))
    if r_pr is None:
        return None
    size = r_pr.find(qn("w:sz"))
    if size is None:
        return None
    try:
        return int(size.get(qn("w:val"))) / 2
    except (TypeError, ValueError):
        return None


def _format_signals(paragraph: Paragraph) -> tuple[float, float | None]:
    total_chars = 0
    bold_chars = 0
    sizes: list[float] = []
    for run in paragraph._p.iter(qn("w:r")):
        text = _xml_run_text(run)
        count = len("".join(text.split()))
        total_chars += count
        if _run_is_bold(run):
            bold_chars += count
        size = _run_font_size(run)
        if size is not None:
            sizes.append(size)
    ratio = bold_chars / total_chars if total_chars else 0.0
    return ratio, max(sizes) if sizes else None


def _heading_level(paragraph: Paragraph) -> int | None:
    style = paragraph.style
    candidates = [
        getattr(style, "name", "") or "",
        getattr(style, "style_id", "") or "",
    ]
    for candidate in candidates:
        match = _HEADING_NAME_RE.search(candidate) or _HEADING_ID_RE.search(candidate)
        if match:
            return int(match.group(1))

    p_pr = paragraph._p.pPr
    if p_pr is not None:
        outline = p_pr.find(qn("w:outlineLvl"))
        if outline is not None:
            try:
                return int(outline.get(qn("w:val"))) + 1
            except (TypeError, ValueError):
                pass
    return None


def _numbering_kind(document: _Document, paragraph: Paragraph) -> str | None:
    p_pr = paragraph._p.pPr
    num_pr = p_pr.find(qn("w:numPr")) if p_pr is not None else None
    style_name = (getattr(paragraph.style, "name", "") or "").lower()
    if num_pr is None:
        if any(token in style_name for token in ("bullet", "маркер")):
            return "ul"
        if any(token in style_name for token in ("number", "нумер")):
            return "ol"
        return None

    num_id_node = num_pr.find(qn("w:numId"))
    level_node = num_pr.find(qn("w:ilvl"))
    if num_id_node is None:
        return "ul"
    num_id = num_id_node.get(qn("w:val"))
    level = level_node.get(qn("w:val")) if level_node is not None else "0"

    try:
        numbering = document.part.numbering_part.element
        abstract_id = None
        for num in numbering.findall(qn("w:num")):
            if num.get(qn("w:numId")) == num_id:
                ref = num.find(qn("w:abstractNumId"))
                abstract_id = ref.get(qn("w:val")) if ref is not None else None
                break
        if abstract_id is not None:
            for abstract in numbering.findall(qn("w:abstractNum")):
                if abstract.get(qn("w:abstractNumId")) != abstract_id:
                    continue
                for level_element in abstract.findall(qn("w:lvl")):
                    if level_element.get(qn("w:ilvl")) == level:
                        fmt = level_element.find(qn("w:numFmt"))
                        value = fmt.get(qn("w:val")) if fmt is not None else ""
                        return "ul" if value == "bullet" else "ol"
    except (AttributeError, KeyError):
        pass
    return "ul"


def _looks_like_unformatted_heading(text: str, next_text: str) -> bool:
    words = text.split()
    return (
        2 <= len(words) <= 10
        and len(text) <= 100
        and not _TERMINAL_PUNCTUATION_RE.search(text)
        and len(next_text) >= 70
    )


def _classify(blocks: list[Block]) -> None:
    body_sizes = [
        block.max_font_size
        for block in blocks
        if block.max_font_size is not None and block.explicit_heading_level is None
    ]
    baseline_size = median(body_sizes) if body_sizes else None

    for position, block in enumerate(blocks):
        if block.explicit_heading_level is not None:
            html_level = min(4, max(2, block.explicit_heading_level))
            block.role = f"h{html_level}"
            block.confidence = 0.99
            block.reason = f"Стиль заголовка Word, рівень {block.explicit_heading_level}"
            continue

        if block.role in ("ul", "ol"):
            block.confidence = 0.99
            block.reason = "Список у DOCX"
            continue

        is_short = len(block.text) <= 120 and len(block.text.split()) <= 14
        if is_short and block.bold_ratio >= 0.8:
            block.role = "h2"
            block.confidence = 0.82
            block.reason = "Окремий короткий абзац майже повністю жирний"
            continue

        if (
            is_short
            and baseline_size is not None
            and block.max_font_size is not None
            and block.max_font_size >= baseline_size + 2
        ):
            block.role = "h2"
            block.confidence = 0.78
            block.reason = "Шрифт помітно більший за основний текст"
            continue

        next_text = blocks[position + 1].text if position + 1 < len(blocks) else ""
        if _looks_like_unformatted_heading(block.text, next_text):
            block.role = "h2"
            block.confidence = 0.55
            block.reason = "Схоже на неоформлений заголовок — перевірте"
            continue

        block.role = "p"
        block.confidence = 0.95
        block.reason = "Звичайний абзац"


def _append_structure_warnings(result: DocumentResult) -> None:
    if not result.blocks:
        return

    total_text_length = sum(len(block.text) for block in result.blocks)
    has_headings = any(block.role.startswith("h") for block in result.blocks)

    if (
        len(result.blocks) == 1
        and total_text_length >= _LONG_SINGLE_BLOCK_THRESHOLD
        and not has_headings
    ):
        result.warnings.append(
            "Документ завантажений суцільним полотном. "
            "Перевірте розбивку на абзаци та заголовки."
        )
    elif total_text_length >= _LONG_DOCUMENT_THRESHOLD and not has_headings:
        result.warnings.append(
            "Заголовки не розпізнані. Перевірте структуру документа."
        )


def parse_docx(data: bytes, filename: str) -> DocumentResult:
    """Parse a DOCX into semantic blocks without changing its wording."""

    _validate_docx_archive(data)
    document = Document(io.BytesIO(data))
    result = DocumentResult(filename=filename)
    table_count = 0

    for item in _iter_document_blocks(document):
        if isinstance(item, Table):
            table_count += 1
            continue

        parts = _extract_parts(item)
        text = "".join(part.text for part in parts).strip()
        if not text:
            continue

        bold_ratio, max_font_size = _format_signals(item)
        role = _numbering_kind(document, item) or "p"
        result.blocks.append(
            Block(
                index=len(result.blocks) + 1,
                text=text,
                parts=parts,
                role=role,
                style_name=getattr(item.style, "name", "") or "",
                explicit_heading_level=_heading_level(item),
                bold_ratio=bold_ratio,
                max_font_size=max_font_size,
            )
        )

    if table_count:
        result.warnings.append(
            f"Знайдено таблиць: {table_count}. У першій версії таблиці пропущено."
        )
    image_count = len(document.inline_shapes)
    if image_count:
        result.warnings.append(
            f"Знайдено зображень: {image_count}. У першій версії зображення пропущено."
        )
    if not result.blocks:
        result.warnings.append("Не знайдено текстових абзаців для конвертації.")

    _classify(result.blocks)
    _append_structure_warnings(result)
    uncertain = sum(1 for block in result.blocks if block.confidence < 0.7)
    if uncertain:
        result.warnings.append(
            f"Неоднозначних блоків: {uncertain}. Перевірте їхні типи перед експортом."
        )
    return result
