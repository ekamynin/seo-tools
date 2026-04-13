import re
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed

BASE_URL = "https://collaborator.pro/ua/api/public/creator/list"


def parse_metric(s) -> float:
    """Parse strings like '2.3 k', '109.25 k', '54', '—' to float."""
    if not s or str(s).strip() in ("—", "–", "-", "", "null"):
        return 0.0
    s = str(s).strip().replace("&nbsp;", "").replace("\xa0", "").replace("\u00a0", "").replace(" ", "")
    multiplier = 1.0
    if s.lower().endswith("k"):
        multiplier = 1_000
        s = s[:-1]
    elif s.lower().endswith("m"):
        multiplier = 1_000_000
        s = s[:-1]
    try:
        return float(s.replace(",", ".")) * multiplier
    except ValueError:
        return 0.0


def parse_price(s):
    """Parse '1 299.00 UAH' → 1299.0"""
    if not s or str(s).strip() in ("—", "–", "-", ""):
        return None
    cleaned = re.sub(r"[^\d.]", "", str(s).replace("&nbsp;", "").replace("\xa0", "").replace(" ", ""))
    try:
        return float(cleaned) if cleaned else None
    except ValueError:
        return None


def _fetch_page(api_key: str, page: int, params: dict) -> dict:
    headers = {"X-Api-Key": api_key}
    p = {**params, "page": page, "per-page": 100}
    resp = requests.get(BASE_URL, headers=headers, params=p, timeout=30)
    resp.raise_for_status()
    return resp.json()


def fetch_all_sites(
    api_key: str,
    dr_min: int = 20,
    traffic_min: int = 15000,
    da_min: int = 15,
    price_min=None,
    price_max=None,
    progress_callback=None,
):
    """Fetch all sites matching base filters. Returns (items, total_count)."""
    params: dict = {
        "ahrefs_dr_min": dr_min,
        "ahrefs_traffic_min": traffic_min,
        "_moz_da_min": da_min,
    }
    if price_min:
        params["_price_min"] = price_min
    if price_max:
        params["_price_max"] = price_max

    first = _fetch_page(api_key, 1, params)
    pagination = first.get("pagination", {})
    total_pages = pagination.get("pageCount", 1)
    total_count = pagination.get("totalCount", 0)
    items = first.get("items", [])

    if progress_callback:
        progress_callback(1, total_pages)

    if total_pages > 1:
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = {
                executor.submit(_fetch_page, api_key, p, params): p
                for p in range(2, total_pages + 1)
            }
            done = 1
            for future in as_completed(futures):
                data = future.result()
                items.extend(data.get("items", []))
                done += 1
                if progress_callback:
                    progress_callback(done, total_pages)

    return items, total_count


def parse_site(item: dict) -> dict:
    """Convert raw API item to a clean dict with numeric values."""
    prices = item.get("prices", [])
    price = None
    price_writing = None
    link_type = None
    if prices:
        price = parse_price(prices[0].get("pricePublication"))
        link_type = prices[0].get("linkType", "")
        raw_spelling = prices[0].get("priceSpelling", "")
        price_writing = parse_price(raw_spelling) if raw_spelling else None

    total_traffic = parse_metric(item.get("traffic", ""))
    organic_traffic = parse_metric(item.get("organicTraffic", ""))
    pct_organic = (organic_traffic / total_traffic * 100) if total_traffic > 0 else 0.0

    return {
        "id": item.get("id"),
        "domain": item.get("name", ""),
        "collaborator_url": item.get("url", ""),
        "categories": item.get("categories", ""),
        "country": item.get("country", ""),
        "dr": parse_metric(item.get("dr", "0")),
        "da_moz": parse_metric(item.get("daMoz", "0")),
        "organic_traffic": organic_traffic,
        "total_traffic": total_traffic,
        "pct_organic": round(pct_organic, 1),
        "referral_domains": parse_metric(item.get("referralDomains", "0")),
        "backlinks": parse_metric(item.get("backlinks", "0")),
        "price": price,
        "price_writing": price_writing,
        "link_type": link_type,
        "site_type": item.get("siteType", ""),
        "domain_zone": item.get("domainZone", ""),
        "placement_speed": item.get("placementSpeed", ""),
    }
