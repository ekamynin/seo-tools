from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class InlinePart:
    """A piece of paragraph text, optionally carrying a hyperlink."""

    text: str
    href: str | None = None


@dataclass
class Block:
    """A semantic document block before it is rendered as HTML."""

    index: int
    text: str
    parts: list[InlinePart]
    role: str = "p"
    confidence: float = 1.0
    reason: str = "Звичайний абзац"
    style_name: str = ""
    explicit_heading_level: int | None = None
    bold_ratio: float = 0.0
    max_font_size: float | None = None


@dataclass
class DocumentResult:
    """Parsed document and non-fatal conversion notes."""

    filename: str
    blocks: list[Block] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
