from __future__ import annotations

import re

from .models import Block, DocumentResult, InlinePart
from .parser import _append_structure_warnings, _classify


_UNORDERED_ITEM_RE = re.compile(r"^\s*[-*•▪◦–—]\s+(.+)$")
_ORDERED_ITEM_RE = re.compile(r"^\s*\d+[.)]\s+(.+)$")


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
