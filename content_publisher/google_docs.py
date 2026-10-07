from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import unquote, urlparse

import requests


_ANY_URL_RE = re.compile(
    r"https?://(?:(?!https?://)[^\s|<>\[\]()])+",
    re.IGNORECASE,
)
_DOC_PATH_RE = re.compile(r"^/document/(?:u/\d+/)?d/([A-Za-z0-9_-]+)")
_SAFE_FILENAME_RE = re.compile(r"[^\w. -]+", re.UNICODE)
_DOCX_CONTENT_TYPES = {
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    "application/octet-stream",
}


class GoogleDocsError(RuntimeError):
    """A user-facing Google Docs download error."""


class GoogleDocsAccessError(GoogleDocsError):
    """The document is not exported because link access is unavailable."""


@dataclass
class GoogleDocsInput:
    """Recognized Google Docs links and input cleanup statistics."""

    links: list[str]
    duplicate_count: int = 0
    invalid_count: int = 0


def extract_google_doc_id(url: str) -> str:
    parsed = urlparse(url.strip())
    if parsed.scheme not in ("http", "https") or parsed.hostname != "docs.google.com":
        raise ValueError("Підтримуються лише посилання Google Docs.")
    match = _DOC_PATH_RE.match(parsed.path)
    if not match:
        raise ValueError("Не вдалося знайти ID Google-документа.")
    return match.group(1)


def analyze_google_doc_links(text: str) -> GoogleDocsInput:
    """Extract unique links and count duplicates and unsupported URLs."""

    links: list[str] = []
    seen_ids: set[str] = set()
    duplicate_count = 0
    invalid_count = 0
    for match in _ANY_URL_RE.finditer(text):
        candidate = match.group(0)
        candidate = candidate.rstrip(".,;:!?'")
        try:
            document_id = extract_google_doc_id(candidate)
        except ValueError:
            invalid_count += 1
            continue
        if document_id in seen_ids:
            duplicate_count += 1
            continue
        seen_ids.add(document_id)
        links.append(candidate)
    return GoogleDocsInput(
        links=links,
        duplicate_count=duplicate_count,
        invalid_count=invalid_count,
    )


def extract_google_doc_links(text: str) -> list[str]:
    """Extract unique Google Docs links from lines, Markdown, or pasted tables."""

    return analyze_google_doc_links(text).links


def _filename_from_headers(headers, document_id: str) -> str:
    disposition = headers.get("content-disposition", "")
    star_match = re.search(
        r"filename\*\s*=\s*UTF-8''([^;]+)", disposition, re.IGNORECASE
    )
    quoted_match = re.search(
        r'filename\s*=\s*"([^"]+)"', disposition, re.IGNORECASE
    )
    raw_name = unquote(star_match.group(1)) if star_match else None
    if not raw_name and quoted_match:
        raw_name = quoted_match.group(1)
    raw_name = raw_name or f"Google_Doc_{document_id[:10]}.docx"
    name = _SAFE_FILENAME_RE.sub("_", Path(raw_name).name).strip(" ._")
    if not name.lower().endswith(".docx"):
        name += ".docx"
    stem = Path(name).stem[:120].rstrip(" ._")
    return f"{stem}.docx" if stem else f"Google_Doc_{document_id[:10]}.docx"


def download_google_doc(url: str, max_size: int) -> tuple[bytes, str]:
    """Download a public/viewable Google Doc through Google's DOCX export."""

    document_id = extract_google_doc_id(url)
    export_url = (
        f"https://docs.google.com/document/d/{document_id}/export?format=docx"
    )
    try:
        response = requests.get(
            export_url,
            stream=True,
            timeout=(8, 45),
            headers={"User-Agent": "SEO-Tools-Content-Publisher/1.0"},
        )
    except requests.RequestException as exc:
        raise GoogleDocsError("Не вдалося завантажити документ.") from exc

    with response:
        if response.status_code in (401, 403, 404):
            raise GoogleDocsAccessError(
                "Немає доступу. Увімкніть «Усі, хто має посилання — читач»."
            )
        try:
            response.raise_for_status()
        except requests.RequestException as exc:
            raise GoogleDocsError(
                f"Google Docs повернув помилку HTTP {response.status_code}."
            ) from exc

        content_type = response.headers.get("content-type", "").split(";", 1)[0].lower()
        if content_type not in _DOCX_CONTENT_TYPES:
            raise GoogleDocsAccessError(
                "Google не віддав DOCX. Перевірте доступ до документа."
            )

        content_length = response.headers.get("content-length")
        if content_length and int(content_length) > max_size:
            raise GoogleDocsError("Документ переевищує ліміт 10 МБ.")

        chunks: list[bytes] = []
        downloaded = 0
        for chunk in response.iter_content(chunk_size=64 * 1024):
            if not chunk:
                continue
            downloaded += len(chunk)
            if downloaded > max_size:
                raise GoogleDocsError("Документ переевищує ліміт 10 МБ.")
            chunks.append(chunk)

        data = b"".join(chunks)
        if not data:
            raise GoogleDocsError("Google повернув порожній документ.")
        return data, _filename_from_headers(response.headers, document_id)
