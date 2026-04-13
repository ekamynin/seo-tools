import asyncio
import aiohttp
from dataclasses import dataclass
from typing import Optional, Callable
from bs4 import BeautifulSoup

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "uk-UA,uk;q=0.9,en;q=0.8",
}


@dataclass
class PageCheckResult:
    url: str
    http_status: Optional[int] = None
    noindex: Optional[bool] = None
    nofollow: Optional[str] = None  # dofollow | nofollow | page-nofollow | не знайдено | —
    error: Optional[str] = None


def _parse_noindex(html: str) -> bool:
    soup = BeautifulSoup(html, "html.parser")
    for meta in soup.find_all("meta"):
        name = (meta.get("name") or meta.get("property") or "").lower()
        if name in ("robots", "googlebot"):
            content = meta.get("content", "").lower()
            if "noindex" in content:
                return True
    return False


def _parse_nofollow(html: str, target_domain: str = "") -> str:
    """
    If target_domain given: find links to that domain and check rel.
    Returns: "dofollow" | "nofollow" | "не знайдено" | "page-nofollow"
    """
    soup = BeautifulSoup(html, "html.parser")

    # Page-level nofollow (affects all links)
    for meta in soup.find_all("meta"):
        name = (meta.get("name") or meta.get("property") or "").lower()
        if name in ("robots", "googlebot"):
            if "nofollow" in meta.get("content", "").lower():
                return "page-nofollow"

    if not target_domain:
        return "—"

    # Find links pointing to target domain
    domain_clean = target_domain.lower().replace("https://", "").replace("http://", "").strip("/")
    links = [
        a for a in soup.find_all("a", href=True)
        if domain_clean in a["href"].lower()
    ]
    if not links:
        return "не знайдено"

    results = []
    for link in links:
        rel = link.get("rel") or []
        rel_str = " ".join(rel).lower() if isinstance(rel, list) else str(rel).lower()
        if "sponsored" in rel_str:
            results.append("sponsored")
        elif "nofollow" in rel_str or "ugc" in rel_str:
            results.append("nofollow")
        else:
            results.append("dofollow")

    if "sponsored" in results:
        return "sponsored"
    unique = set(results)
    if len(unique) > 1:
        return "змішано"
    if "nofollow" in unique:
        return "nofollow"
    return "dofollow"


async def _check_one(
    session: aiohttp.ClientSession,
    url: str,
    target_domain: str,
) -> PageCheckResult:
    try:
        async with session.get(
            url, headers=HEADERS, timeout=aiohttp.ClientTimeout(total=15),
            allow_redirects=True, ssl=False,
        ) as resp:
            status = resp.status
            try:
                html = await resp.text(errors="replace")
            except Exception:
                return PageCheckResult(url=url, http_status=status, error="Не вдалось прочитати HTML")

            noindex  = _parse_noindex(html)
            nofollow = _parse_nofollow(html, target_domain)
            return PageCheckResult(url=url, http_status=status, noindex=noindex, nofollow=nofollow)

    except asyncio.TimeoutError:
        return PageCheckResult(url=url, error="Timeout")
    except Exception as e:
        return PageCheckResult(url=url, error=str(e)[:80])


async def check_pages(
    urls: list[str],
    target_domain: str = "",
    concurrency: int = 5,
    progress_callback: Optional[Callable[[int, int], None]] = None,
) -> list[PageCheckResult]:
    semaphore = asyncio.Semaphore(concurrency)
    results: list[PageCheckResult] = []

    async def bounded(url: str):
        async with semaphore:
            result = await _check_one(session, url, target_domain)
        results.append(result)
        if progress_callback:
            progress_callback(len(results), len(urls))

    connector = aiohttp.TCPConnector(limit=concurrency, ssl=False)
    async with aiohttp.ClientSession(connector=connector) as session:
        await asyncio.gather(*[bounded(u) for u in urls])

    order = {url: i for i, url in enumerate(urls)}
    results.sort(key=lambda r: order.get(r.url, 0))
    return results
