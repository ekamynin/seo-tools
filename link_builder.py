import random
import re
import pandas as pd

# Translation map: Collaborator English category → Ukrainian
CATEGORY_TRANSLATIONS = {
    "Business and Finance":               "Бізнес та фінанси",
    "City portals":                       "Міські портали",
    "Computer games":                     "Комп'ютерні ігри",
    "Construction and repair":            "Будівництво та ремонт",
    "Cooking":                            "Кулінарія",
    "Cryptocurrencies":                   "Криптовалюти",
    "Culture and art":                    "Культура та мистецтво",
    "Dacha":                              "Дача",
    "Education and Science":              "Освіта та наука",
    "Electronics and Technology":         "Електроніка та технології",
    "Entertainment and hobbies":          "Розваги та хобі",
    "Fashion and beauty":                 "Мода та краса",
    "Furniture and interior":             "Меблі та інтер'єр",
    "Health and medicine":                "Здоров'я та медицина",
    "Home and family":                    "Дім та сім'я",
    "Internet":                           "Інтернет",
    "Law and jurisprudence":              "Право та юриспруденція",
    "laws":                               "Право та юриспруденція",  # normalize duplicate
    "Logistics and cargo transportation": "Логістика та вантажоперевезення",
    "Manufacturing and agriculture":      "Виробництво та агробізнес",
    "Marketing":                          "Маркетинг",
    "Media (News)":                       "ЗМІ (Новини)",
    "Mobile technology":                  "Мобільні технології",
    "Other":                              "Інше",
    "Pets":                               "Домашні тварини",
    "politics":                           "Політика",
    "Programs (Soft)":                    "Програмне забезпечення",
    "Psychology":                         "Психологія",
    "Real estate":                        "Нерухомість",
    "SEO":                                "SEO",
    "Society":                            "Суспільство",
    "Sport":                              "Спорт",
    "Technologies":                       "Технології",
    "Tourism and travel":                 "Туризм та подорожі",
    "Web design":                         "Веб-дизайн",
    "Web development":                    "Веб-розробка",
    "Work":                               "Робота",
    # Already Ukrainian — keep as-is
    "Авто та мото":                       "Авто та мото",
    "Астрологія та езотерика":            "Астрологія та езотерика",
    "Лайфстал":                           "Лайфстайл",
    "Шопінг (сайти для покупок, купони)":  "Шопінг",
}

# Reverse map: Ukrainian display name → list of original Collaborator values
def _build_reverse_map():
    reverse = {}
    for orig, ua in CATEGORY_TRANSLATIONS.items():
        reverse.setdefault(ua, []).append(orig)
    return reverse

REVERSE_CATEGORY_MAP = _build_reverse_map()


def _split_categories(raw: str) -> list[str]:
    """Split categories string, handling parentheses with commas inside."""
    # Replace commas inside parentheses temporarily
    clean = re.sub(r"\(([^)]*),([^)]*)\)", lambda m: f"({m.group(1)}COMMA{m.group(2)})", raw)
    parts = [p.strip().replace("COMMA", ",") for p in clean.split(",")]
    return [p for p in parts if p]


def get_all_categories(df: pd.DataFrame) -> list[str]:
    """Extract sorted unique Ukrainian category names from loaded dataframe."""
    ua_cats = set()
    for raw in df["categories"].dropna():
        for cat in _split_categories(raw):
            ua = CATEGORY_TRANSLATIONS.get(cat.strip(), cat.strip())
            if ua:
                ua_cats.add(ua)
    return sorted(ua_cats)


def filter_by_keywords(df: pd.DataFrame, keywords: list[str]) -> pd.DataFrame:
    """Filter by free-text keywords against categories field (for advanced tab)."""
    if not keywords:
        return df
    cats_lower = df["categories"].str.lower().fillna("")
    mask = cats_lower.apply(lambda c: any(kw.lower() in c for kw in keywords))
    return df[mask]


def filter_by_categories(df: pd.DataFrame, selected_ua: list[str]) -> pd.DataFrame:
    """Keep only sites that have at least one of the selected Ukrainian categories."""
    if not selected_ua:
        return df
    # Build set of all original Collaborator values for selected UA categories
    originals = set()
    for ua in selected_ua:
        for orig in REVERSE_CATEGORY_MAP.get(ua, [ua]):
            originals.add(orig.lower())

    def matches(raw):
        return any(c.strip().lower() in originals for c in _split_categories(raw))

    mask = df["categories"].fillna("").apply(matches)
    return df[mask]


def apply_hard_filters(df: pd.DataFrame, criteria: dict, strict: bool = True) -> pd.DataFrame:
    """Apply threshold filters. Returns filtered copy.
    strict=True  → also removes red-flag sites (Tab 1).
    strict=False → only applies criteria set by the user (Tab 2).
    """
    mask = pd.Series(True, index=df.index)

    if criteria.get("dr_min") is not None:
        mask &= df["dr"] >= criteria["dr_min"]
    if criteria.get("organic_traffic_min") is not None:
        mask &= df["organic_traffic"] >= criteria["organic_traffic_min"]
    if criteria.get("pct_organic_min") is not None:
        mask &= df["pct_organic"] >= criteria["pct_organic_min"]
    if criteria.get("total_traffic_min") is not None:
        mask &= df["total_traffic"] >= criteria["total_traffic_min"]
    if criteria.get("ukraine_only"):
        mask &= df["country"].str.contains("Ukraine", case=False, na=False) | df["domain"].str.endswith(".ua")
    if criteria.get("price_max") is not None:
        mask &= df["price"] <= criteria["price_max"]
    if criteria.get("price_min") is not None:
        mask &= df["price"] >= criteria["price_min"]

    # Exclude already-used domains
    excluded = [d.strip().lower() for d in (criteria.get("excluded_domains") or []) if d.strip()]
    if excluded:
        mask &= ~df["domain"].str.lower().isin(excluded)

    # Red flag: very high DR + near-zero organic traffic (manipulated metrics)
    # Only applied in strict mode (Tab 1). Tab 2 trusts the user's own judgement.
    if strict:
        red_flag = (df["dr"] > 50) & (df["organic_traffic"] < 500)
        mask &= ~red_flag

    # Price must exist (required for budget calculations)
    mask &= df["price"].notna()

    return df[mask].copy()


def score_sites(df: pd.DataFrame) -> pd.DataFrame:
    """Add quality score (DR + traffic only, no price). Higher = better quality."""
    df = df.copy()
    dr_max = df["dr"].max() or 1
    traffic_max = df["organic_traffic"].max() or 1

    df["score"] = (
        (df["dr"] / dr_max) * 0.50
        + (df["organic_traffic"] / traffic_max) * 0.50
    )
    return df


def select_donors(df: pd.DataFrame, quantity: int, budget: float) -> pd.DataFrame:
    """Select exactly `quantity` donors within budget.
    - Random noise on scores → different results on each click.
    - Quality-first greedy pick.
    - If quantity not met: drop most expensive, retry with cheaper alternatives.
    """
    df = df[df["price"].notna()].copy()

    # ±15% random noise → variety between runs
    df["score_r"] = df["score"].apply(lambda s: s * (1 + random.uniform(-0.15, 0.15)))
    df = df.sort_values("score_r", ascending=False).reset_index(drop=True)

    def _greedy(pool, qty, bdg, excluded):
        sel, spent = [], 0.0
        for _, row in pool.iterrows():
            if len(sel) >= qty:
                break
            if row["domain"] in excluded:
                continue
            if spent + row["price"] <= bdg:
                sel.append(row)
                spent += row["price"]
                excluded.add(row["domain"])
        return sel

    excluded = set()
    selected = _greedy(df, quantity, budget, excluded)

    # Iteratively drop most expensive and re-fill until quantity met
    for _ in range(quantity * 3):
        if len(selected) >= quantity:
            break
        if not selected:
            break

        most_exp = max(selected, key=lambda r: r["price"])
        selected = [r for r in selected if r["domain"] != most_exp["domain"]]
        excluded.add(most_exp["domain"])

        spent = sum(r["price"] for r in selected)
        used = {r["domain"] for r in selected} | excluded
        pool = df[~df["domain"].isin(used)].reset_index(drop=True)
        extra = _greedy(pool, quantity - len(selected), budget - spent, used)
        selected.extend(extra)

    # Rebuild with cumulative price
    result, cumulative = [], 0.0
    for row in selected:
        cumulative += row["price"]
        r = row.copy()
        r["cumulative_price"] = round(cumulative, 2)
        result.append(r)

    return pd.DataFrame(result) if result else pd.DataFrame()


def build_why_suitable(row: pd.Series) -> str:
    parts = []
    if row["dr"] >= 40:
        parts.append(f"DR {int(row['dr'])} (відмінний)")
    elif row["dr"] >= 30:
        parts.append(f"DR {int(row['dr'])} (добрий)")
    else:
        parts.append(f"DR {int(row['dr'])}")

    ot = row["organic_traffic"]
    if ot >= 50_000:
        parts.append(f"органічний трафік {int(ot):,} (дуже високий)")
    elif ot >= 20_000:
        parts.append(f"органічний трафік {int(ot):,} (відмінний)")
    else:
        parts.append(f"органічний трафік {int(ot):,}")

    parts.append(f"ціна {int(row['price'])} грн")
    return "; ".join(parts)
