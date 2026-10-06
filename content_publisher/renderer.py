from __future__ import annotations

import html
import io
import re
import zipfile
from pathlib import Path
from urllib.parse import urlparse

from .models import Block, DocumentResult, InlinePart


ALLOWED_ROLES = {"p", "h2", "h3", "h4", "ul", "ol"}
_UNSAFE_FILENAME_RE = re.compile(r"[^\w. -]+", re.UNICODE)


def _safe_href(href: str | None) -> str | None:
    if not href:
        return None
    href = href.strip()
    parsed = urlparse(href)
    if parsed.scheme and parsed.scheme.lower() not in ("http", "https", "mailto"):
        return None
    if href.startswith("//"):
        return None
    return href


def _render_part(part: InlinePart, profile: str) -> str:
    text = html.escape(part.text, quote=False)
    href = _safe_href(part.href)
    if not href:
        return text
    safe_url = html.escape(href, quote=True)
    if profile == "leroy_merlin":
        return (
            f'<a href="{safe_url}" data-gjs-tagName="a" data-gjs-type="text">'
            f'<font color="#78BE20">{text}</font></a>'
        )
    return f'<a href="{safe_url}">{text}</a>'


def _render_inline(block: Block, profile: str) -> str:
    return "".join(_render_part(part, profile) for part in block.parts)


def render_html(blocks: list[Block], profile: str = "default") -> str:
    """Render only an HTML fragment. Bold formatting is intentionally discarded."""

    if profile not in ("default", "leroy_merlin"):
        raise ValueError(f"Unknown rendering profile: {profile}")

    lines: list[str] = []
    open_list: str | None = None

    def close_list() -> None:
        nonlocal open_list
        if open_list:
            lines.append(f"</{open_list}>")
            open_list = None

    for block in blocks:
        role = block.role if block.role in ALLOWED_ROLES else "p"
        content = _render_inline(block, profile)

        if role in ("ul", "ol"):
            if open_list != role:
                close_list()
                open_list = role
                lines.append(f"<{role}>")
            if profile == "leroy_merlin":
                lines.append(
                    f'<li data-gjs-tagName="li" data-gjs-type="text">{content}</li>'
                )
            else:
                lines.append(f"<li>{content}</li>")
            continue

        close_list()
        if role.startswith("h"):
            lines.append(f"<{role}>{content}</{role}>")
        elif profile == "leroy_merlin":
            lines.append(f"<p>{content}<br><br></p>")
        else:
            lines.append(f"<p>{content}</p>")

    close_list()
    return "\n".join(lines)


def validate_html(fragment: str, profile: str = "default") -> list[str]:
    errors: list[str] = []
    lowered = fragment.lower()
    if "<strong" in lowered or re.search(r"<b(?:\s|>)", lowered):
        errors.append("Знайдено заборонене жирне форматування.")
    if any(tag in lowered for tag in ("<html", "<head", "<body", "<!doctype")):
        errors.append("Результат містить зайву HTML-обгортку.")

    if profile == "default":
        if "data-gjs-" in fragment or "<font" in lowered:
            errors.append("У звичайний HTML потрапила розмітка Leroy Merlin.")
    else:
        anchors = re.findall(r"<a\b[^>]*>.*?</a>", fragment, flags=re.IGNORECASE | re.DOTALL)
        for anchor in anchors:
            if 'data-gjs-tagName="a"' not in anchor or 'data-gjs-type="text"' not in anchor:
                errors.append("Посилання без обов’язкових атрибутів Leroy Merlin.")
                break
            if '<font color="#78BE20">' not in anchor:
                errors.append("Анкор посилання не має кольору Leroy Merlin.")
                break
        paragraphs = re.findall(r"<p\b[^>]*>.*?</p>", fragment, flags=re.IGNORECASE | re.DOTALL)
        if any(not paragraph.endswith("<br><br></p>") for paragraph in paragraphs):
            errors.append("Не кожен абзац завершується <br><br>.")
    return errors


def _output_name(source_name: str) -> str:
    stem = _UNSAFE_FILENAME_RE.sub("_", Path(source_name).stem).strip(" ._") or "document"
    return f"{stem}.html"


def build_zip(results: list[DocumentResult], profile: str = "default") -> bytes:
    output = io.BytesIO()
    used_names: set[str] = set()
    with zipfile.ZipFile(output, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for result in results:
            base_name = _output_name(result.filename)
            name = base_name
            suffix = 2
            while name.lower() in used_names:
                name = f"{Path(base_name).stem}_{suffix}.html"
                suffix += 1
            used_names.add(name.lower())
            archive.writestr(name, render_html(result.blocks, profile))
    return output.getvalue()
