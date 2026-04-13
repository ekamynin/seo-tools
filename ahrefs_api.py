import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date


AHREFS_BASE = "https://api.ahrefs.com/v3/site-explorer"


def analyze_traffic_health(points: list) -> dict:
    """Analyze 24 months of monthly organic traffic for suspicious patterns.

    Pattern 1 — Spike (накрутка):
        Near-zero history for most of the period, then a sudden spike.

    Pattern 2 — Penalty (вбитий апдейтом):
        Had meaningful traffic, then crashed and stayed low for 3+ months.

    Returns {"status": "ok" | "spike" | "penalty", "label": ""}
    """
    if len(points) < 6:
        return {"status": "ok", "label": ""}

    recent   = points[-3:]   # last 3 months
    last_6   = points[-6:]   # last 6 months
    historical = points[:-6] # older history (months 7+)

    recent_avg = sum(recent) / len(recent)
    recent_max = max(last_6) if last_6 else 0
    all_peak   = max(points) if points else 0

    # Pattern 1 — Spike
    if historical:
        hist_sorted = sorted(historical)
        hist_median = hist_sorted[len(hist_sorted) // 2]
        if hist_median < 1000 and recent_max > max(hist_median, 500) * 5:
            return {"status": "spike", "label": "⚠️ Підозрілий трафік"}

    # Pattern 2 — Penalty
    if all_peak > 5000 and all(v < all_peak * 0.3 for v in recent) and recent_avg < all_peak * 0.3:
        return {"status": "penalty", "label": "📉 Трафік впав після апдейту"}

    return {"status": "ok", "label": ""}


def _fetch_domain_metrics(api_key: str, domain: str) -> dict:
    """Fetch DR, organic traffic, and 24-month traffic history for a domain."""
    headers = {"Authorization": f"Bearer {api_key}"}
    today = date.today()

    try:
        dr_resp = requests.get(
            f"{AHREFS_BASE}/domain-rating",
            headers=headers,
            params={"target": domain, "date": today.isoformat()},
            timeout=15,
        )
        dr_data = dr_resp.json() if dr_resp.ok else {}

        tr_resp = requests.get(
            f"{AHREFS_BASE}/metrics",
            headers=headers,
            params={"target": domain, "date": today.isoformat(), "mode": "subdomains"},
            timeout=15,
        )
        tr_data = tr_resp.json() if tr_resp.ok else {}

        # 24-month traffic history
        date_from = date(today.year - 2, today.month, 1).isoformat()
        hist_resp = requests.get(
            f"{AHREFS_BASE}/metrics-history",
            headers=headers,
            params={
                "target": domain,
                "date_from": date_from,
                "mode": "subdomains",
                "history_grouping": "monthly",
                "select": "date,org_traffic",
            },
            timeout=20,
        )
        hist_data = hist_resp.json() if hist_resp.ok else {}
        points = [m.get("org_traffic", 0) for m in hist_data.get("metrics", [])]
        health = analyze_traffic_health(points)

        return {
            "domain": domain,
            "dr": dr_data.get("domain_rating", {}).get("domain_rating"),
            "org_traffic": tr_data.get("metrics", {}).get("org_traffic"),
            "traffic_status": health["status"],
            "traffic_label": health["label"],
        }
    except Exception:
        return {
            "domain": domain,
            "dr": None,
            "org_traffic": None,
            "traffic_status": "ok",
            "traffic_label": "",
        }


def enrich_with_ahrefs(api_key: str, domains: list) -> dict:
    """Fetch DR, organic traffic, and traffic health for a list of domains.
    Returns: {domain: {dr, org_traffic, traffic_status, traffic_label}}
    """
    result = {}
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {
            executor.submit(_fetch_domain_metrics, api_key, d): d for d in domains
        }
        for future in as_completed(futures):
            data = future.result()
            result[data["domain"]] = {
                "dr":             data["dr"],
                "org_traffic":    data["org_traffic"],
                "traffic_status": data.get("traffic_status", "ok"),
                "traffic_label":  data.get("traffic_label", ""),
            }
    return result
