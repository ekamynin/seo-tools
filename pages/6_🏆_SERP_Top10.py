import io
import base64
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import requests
import streamlit as st

st.set_page_config(
    page_title="SERP Top 10",
    page_icon="🏆",
    layout="wide",
)

DATAFORSEO_LOGIN = st.secrets.get("DATAFORSEO_LOGIN", "")
DATAFORSEO_PASSWORD = st.secrets.get("DATAFORSEO_PASSWORD", "")

KEYWORD_LIMIT = 200
BATCH_SIZE = 10

LOCATIONS = {
    "🇺🇦 Україна (UK)": {
        "location_code": 2804, "language_code": "uk",
        "cities": {
            "🌍 Вся країна": None,
            "🏙 Київ": "Kyiv,Ukraine",
            "🏙 Львів": "Lviv,Lviv Oblast,Ukraine",
            "🏙 Харків": "Kharkiv,Kharkiv Oblast,Ukraine",
            "🏙 Одеса": "Odessa,Odessa Oblast,Ukraine",
            "🏙 Дніпро": "Dnipro,Dnipropetrovsk Oblast,Ukraine",
            "🏙 Запоріжжя": "Zaporizhzhia,Zaporizhzhia Oblast,Ukraine",
            "🏙 Вінниця": "Vinnytsia,Vinnytsia Oblast,Ukraine",
            "🏙 Полтава": "Poltava,Poltava Oblast,Ukraine",
            "🏙 Івано-Франківськ": "Ivano-Frankivsk,Ivano-Frankivsk Oblast,Ukraine",
        },
    },
    "🇺🇦 Україна (RU)": {
        "location_code": 2804, "language_code": "ru",
        "cities": {
            "🌍 Вся країна": None,
            "🏙 Київ": "Kyiv,Ukraine",
            "🏙 Львів": "Lviv,Lviv Oblast,Ukraine",
            "🏙 Харків": "Kharkiv,Kharkiv Oblast,Ukraine",
            "🏙 Одеса": "Odessa,Odessa Oblast,Ukraine",
            "🏙 Дніпро": "Dnipro,Dnipropetrovsk Oblast,Ukraine",
        },
    },
    "🇺🇸 США (EN)": {"location_code": 2840, "language_code": "en", "cities": {}},
    "🇵🇱 Польща (PL)": {"location_code": 2616, "language_code": "pl", "cities": {}},
}

SERP_URL = "https://api.dataforseo.com/v3/serp/google/organic/live/regular"


def _headers() -> dict:
    creds = base64.b64encode(f"{DATAFORSEO_LOGIN}:{DATAFORSEO_PASSWORD}".encode()).decode()
    return {"Authorization": f"Basic {creds}", "Content-Type": "application/json"}


def _fetch_batch(batch: list[str], location_code: int, language_code: str, location_name: str | None = None) -> list[dict]:
    def _task(kw: str) -> dict:
        t = {"keyword": kw, "language_code": language_code, "depth": 10}
        if location_name:
            t["location_name"] = location_name
        else:
            t["location_code"] = location_code
        return t
    payload = [_task(kw) for kw in batch]
    resp = requests.post(SERP_URL, json=payload, headers=_headers(), timeout=60)
    resp.raise_for_status()
    data = resp.json()
    if data.get("status_code") != 20000:
        raise Exception(f"API error {data.get('status_code')}: {data.get('status_message')}")
    return data.get("tasks", [])


def fetch_serp(keywords: list[str], location_code: int, language_code: str, location_name: str | None = None, progress_callback=None) -> list[dict]:
    batches = [keywords[i:i + BATCH_SIZE] for i in range(0, len(keywords), BATCH_SIZE)]
    all_tasks: list[dict] = []
    fetch_errors: list[str] = []
    done_count = 0

    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {
            executor.submit(_fetch_batch, batch, location_code, language_code, location_name): batch
            for batch in batches
        }
        for future in as_completed(futures):
            try:
                all_tasks.extend(future.result())
            except Exception as e:
                fetch_errors.append(str(e))
            done_count += 1
            if progress_callback:
                progress_callback(done_count, len(batches))

    results = []
    for task in all_tasks:
        task_result = (task.get("result") or [{}])[0]
        keyword = task_result.get("keyword", "")
        if not keyword:
            continue
        items = task_result.get("items") or []

        organic: dict[int, dict] = {}
        detail_rows: list[dict] = []
        ai = {"present": False, "text": "", "sources": []}

        for item in items:
            t = item.get("type")
            if t == "organic":
                rank = item.get("rank_group")
                if rank and 1 <= rank <= 10:
                    domain = item.get("domain", "")
                    organic[rank] = {
                        "domain": domain,
                        "url": item.get("url", ""),
                        "title": item.get("title", ""),
                    }
                    detail_rows.append({
                        "Ключ": keyword,
                        "Позиція": rank,
                        "Домен": domain,
                        "URL": item.get("url", ""),
                        "Заголовок": item.get("title", ""),
                    })
            elif t == "ai_overview":
                ai["present"] = True
                ai["text"] = item.get("text") or item.get("description") or ""
                sources = item.get("items") or []
                ai["sources"] = [s.get("url", "") for s in sources if s.get("url")]

        results.append({
            "keyword": keyword,
            "organic": organic,
            "ai": ai,
            "detail_rows": detail_rows,
        })

    return results, fetch_errors


# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("## ⚙️ Налаштування")
    st.divider()
    location_label = st.selectbox("Локація / Мова", list(LOCATIONS.keys()))
    loc = LOCATIONS[location_label]

    cities = loc.get("cities", {})
    location_name: str | None = None
    if cities:
        city_label = st.selectbox("Місто", list(cities.keys()))
        location_name = cities[city_label]

    st.divider()
    st.caption("SERP Top 10 v1.0")


# ── Main UI ───────────────────────────────────────────────────────────────────
st.title("🏆 SERP Top 10")
st.caption("Хто стоїть в топ-10 органіки по твоїм ключам. Живі дані через DataForSEO.")

if not (DATAFORSEO_LOGIN and DATAFORSEO_PASSWORD):
    st.error("❌ Не знайдено DATAFORSEO_LOGIN / DATAFORSEO_PASSWORD у секретах.")
    st.stop()

# ── Input ─────────────────────────────────────────────────────────────────────
raw = st.text_area(
    "Ключові слова (по одному на рядок)",
    height=250,
    placeholder="купити iphone 16\nнавушники бездротові\nноутбук для роботи",
)

keywords_raw = [kw.strip() for kw in raw.splitlines() if kw.strip()]
keywords = list(dict.fromkeys(keywords_raw))

if len(keywords_raw) > len(keywords):
    st.caption(f"ℹ️ Видалено {len(keywords_raw) - len(keywords)} дублікатів.")

if len(keywords) > KEYWORD_LIMIT:
    st.error(f"Максимум {KEYWORD_LIMIT} ключів за раз. Зараз: {len(keywords)}.")
    keywords = keywords[:KEYWORD_LIMIT]

if keywords:
    c1, c2 = st.columns(2)
    c1.metric("Ключів", len(keywords))
    c2.metric("Орієнтовна вартість", f"~${len(keywords) * 0.006:.2f}")

run = st.button(
    "🔍 Аналізувати видачу",
    type="primary",
    disabled=not keywords,
    use_container_width=True,
)

if run:
    batches_count = -(-len(keywords) // BATCH_SIZE)
    progress = st.progress(0.0, text="Запит до DataForSEO…")

    results, fetch_errors = fetch_serp(
        keywords=keywords,
        location_code=loc["location_code"],
        language_code=loc["language_code"],
        location_name=location_name,
        progress_callback=lambda done, total: progress.progress(
            done / total,
            text=f"Батч {done}/{total}…",
        ),
    )

    progress.empty()
    for err in fetch_errors:
        st.warning(f"⚠️ Помилка: {err}")

    if not results:
        st.warning("⚠️ Даних не отримано.")
        st.stop()

    # ── Build DataFrames ──────────────────────────────────────────────────

    # 1. Pivot: Ключ | #1 | #2 | ... | #10 | AI Overview
    pivot_rows = []
    for r in results:
        row = {"Ключ": r["keyword"]}
        for pos in range(1, 11):
            row[f"#{pos}"] = r["organic"].get(pos, {}).get("domain", "")
        row["AI Overview"] = "✅" if r["ai"]["present"] else "—"
        pivot_rows.append(row)
    df_pivot = pd.DataFrame(pivot_rows)
    # Restore original keyword order
    kw_order = {kw: i for i, kw in enumerate(keywords)}
    df_pivot = df_pivot.sort_values("Ключ", key=lambda s: s.map(kw_order)).reset_index(drop=True)

    # 2. Detailed flat table
    detail_rows = []
    for r in results:
        detail_rows.extend(r["detail_rows"])
    df_detail = pd.DataFrame(detail_rows) if detail_rows else pd.DataFrame()

    # 3. AI Overview sheet
    ai_rows = []
    for r in results:
        if r["ai"]["present"]:
            row = {
                "Ключ": r["keyword"],
                "Текст AIO": r["ai"]["text"],
            }
            for idx, src in enumerate(r["ai"]["sources"], 1):
                row[f"Джерело {idx}"] = src
            ai_rows.append(row)
    df_ai = pd.DataFrame(ai_rows) if ai_rows else pd.DataFrame()

    # 4. Domain frequency
    domain_stats: dict[str, dict] = defaultdict(lambda: {"keywords": [], "positions": []})
    for r in results:
        for pos, info in r["organic"].items():
            d = info["domain"]
            domain_stats[d]["keywords"].append(r["keyword"])
            domain_stats[d]["positions"].append(pos)

    domain_rows = []
    for d, stats in domain_stats.items():
        domain_rows.append({
            "Домен": d,
            "К-сть ключів": len(stats["keywords"]),
            "Середня позиція": round(sum(stats["positions"]) / len(stats["positions"]), 1),
            "Позиції": ", ".join(str(p) for p in sorted(stats["positions"])),
            "Ключі": ", ".join(stats["keywords"]),
        })
    df_domains = (
        pd.DataFrame(domain_rows)
        .sort_values("К-сть ключів", ascending=False)
        .reset_index(drop=True)
    )

    # ── Metrics ───────────────────────────────────────────────────────────
    st.divider()
    aio_count = sum(1 for r in results if r["ai"]["present"])
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("🔑 Ключів оброблено", len(results))
    m2.metric("🤖 З AI Overview", aio_count)
    m3.metric("🏠 Унікальних доменів", len(domain_stats))
    m4.metric("📊 Без результатів", len(keywords) - len(results))

    # ── Pivot table on screen ─────────────────────────────────────────────
    st.caption("Топ-10 доменів для кожного ключа. Відсортовано за порядком введення.")
    st.dataframe(df_pivot, use_container_width=True, hide_index=True)

    # ── Excel ─────────────────────────────────────────────────────────────
    st.divider()
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        df_pivot.to_excel(writer, index=False, sheet_name="Топ-10")
        if not df_detail.empty:
            df_detail.to_excel(writer, index=False, sheet_name="Детально")
        if not df_ai.empty:
            df_ai.to_excel(writer, index=False, sheet_name="AI Overview")
        df_domains.to_excel(writer, index=False, sheet_name="Домени")

        for sheet_name in writer.sheets:
            ws = writer.sheets[sheet_name]
            for col_cells in ws.columns:
                max_len = max((len(str(cell.value or "")) for cell in col_cells), default=10)
                ws.column_dimensions[col_cells[0].column_letter].width = min(max_len + 4, 60)

    buf.seek(0)
    st.download_button(
        "📥 Завантажити Excel",
        data=buf,
        file_name=f"serp_top10_{loc['location_code']}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        use_container_width=True,
    )
