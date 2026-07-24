import io
import base64
import math
from datetime import date

import pandas as pd
import requests
import streamlit as st

st.set_page_config(
    page_title="Keyword Volume History",
    page_icon="📈",
    layout="wide",
)

DATAFORSEO_LOGIN = st.secrets.get("DATAFORSEO_LOGIN", "")
DATAFORSEO_PASSWORD = st.secrets.get("DATAFORSEO_PASSWORD", "")

KEYWORD_LIMIT = 500
BATCH_SIZE = 100

LOCATIONS = {
    "🇺🇦 Україна (UK)": {"location_code": 2804, "language_code": "uk"},
    "🇺🇦 Україна (RU)": {"location_code": 2804, "language_code": "ru"},
    "🇺🇸 США (EN)": {"location_code": 2840, "language_code": "en"},
    "🇵🇱 Польща (PL)": {"location_code": 2616, "language_code": "pl"},
}


def _headers() -> dict:
    creds = base64.b64encode(f"{DATAFORSEO_LOGIN}:{DATAFORSEO_PASSWORD}".encode()).decode()
    return {"Authorization": f"Basic {creds}", "Content-Type": "application/json"}


def fetch_volume(
    keywords: list[str],
    location_code: int,
    language_code: str,
    date_from: str,
    date_to: str,
    progress_callback=None,
) -> list[dict]:
    api_url = "https://api.dataforseo.com/v3/keywords_data/google_ads/search_volume/live"
    results = []
    total_batches = math.ceil(len(keywords) / BATCH_SIZE)

    for batch_idx, i in enumerate(range(0, len(keywords), BATCH_SIZE)):
        batch = keywords[i:i + BATCH_SIZE]
        payload = [{
            "keywords": batch,
            "location_code": location_code,
            "language_code": language_code,
            "date_from": date_from,
            "date_to": date_to,
        }]
        resp = requests.post(api_url, json=payload, headers=_headers(), timeout=60)
        resp.raise_for_status()
        data = resp.json()

        if data.get("status_code") != 20000:
            raise Exception(f"API error {data.get('status_code')}: {data.get('status_message')}")

        for task in data.get("tasks") or []:
            if task.get("status_code") != 20000:
                continue
            for item in task.get("result") or []:
                results.append(item)

        if progress_callback:
            progress_callback(batch_idx + 1, total_batches)

    return results


# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("## ⚙️ Налаштування")
    st.divider()

    location_label = st.selectbox("Локація / Мова", list(LOCATIONS.keys()))
    loc = LOCATIONS[location_label]

    volume_only = st.toggle(
        "Тільки частота",
        value=False,
        help="Показати лише поточний обсяг + CPC без помісячної розбивки.",
    )

    today = date.today()
    if not volume_only:
        period = st.selectbox(
            "Період",
            ["Останні 12 місяців", "Останні 24 місяці", "2026", "2025", "2024", "2023"],
        )
        if period == "Останні 12 місяців":
            date_from = date(today.year - 1, today.month, 1).strftime("%Y-%m-%d")
            date_to = date(today.year, today.month, 1).strftime("%Y-%m-%d")
        elif period == "Останні 24 місяці":
            date_from = date(today.year - 2, today.month, 1).strftime("%Y-%m-%d")
            date_to = date(today.year, today.month, 1).strftime("%Y-%m-%d")
        else:
            year = int(period)
            date_from = f"{year}-01-01"
            date_to = f"{year}-12-01"
        st.caption(f"Період: {date_from} → {date_to}")
    else:
        # Last 12 months — needed by API but won't be shown
        date_from = date(today.year - 1, today.month, 1).strftime("%Y-%m-%d")
        date_to = date(today.year, today.month, 1).strftime("%Y-%m-%d")

    st.divider()
    st.caption("Keyword Volume History v1.1")


# ── Main UI ───────────────────────────────────────────────────────────────────
st.title("📈 Keyword Volume History")
st.caption("Помісячна динаміка пошукового попиту. Дані Google Ads через DataForSEO.")

if not (DATAFORSEO_LOGIN and DATAFORSEO_PASSWORD):
    st.error("❌ Не знайдено DATAFORSEO_LOGIN / DATAFORSEO_PASSWORD у секретах.")
    st.stop()

# ── Input ─────────────────────────────────────────────────────────────────────
raw = st.text_area(
    "Ключові слова (по одному на рядок)",
    height=250,
    placeholder="купити iphone\nнавушники бездротові\nноутбук для роботи",
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
    c2.metric("Орієнтовна вартість", f"~${len(keywords) * 0.00075:.3f}")

run = st.button(
    "📊 Отримати дані",
    type="primary",
    disabled=not keywords,
    use_container_width=True,
)

if run:
    progress = st.progress(0.0, text="Запит до DataForSEO…")

    try:
        raw_results = fetch_volume(
            keywords=keywords,
            location_code=loc["location_code"],
            language_code=loc["language_code"],
            date_from=date_from,
            date_to=date_to,
            progress_callback=lambda done, total: progress.progress(
                done / total, text=f"Батч {done}/{total}…",
            ),
        )
    except Exception as e:
        progress.empty()
        st.error(f"❌ Помилка: {e}")
        st.stop()

    progress.empty()

    if not raw_results:
        st.warning("⚠️ DataForSEO не повернув даних. Перевір ключі або налаштування.")
        st.stop()

    st.divider()

    # ── MODE: тільки частота ──────────────────────────────────────────────
    if volume_only:
        simple_rows = []
        for item in raw_results:
            simple_rows.append({
                "Ключ": item.get("keyword", ""),
                "Обсяг": item.get("search_volume") or 0,
                "CPC ($)": round(item.get("cpc") or 0, 2),
                "Конкуренція": item.get("competition_index") or 0,
            })
        df_simple = (
            pd.DataFrame(simple_rows)
            .sort_values("Обсяг", ascending=False)
            .reset_index(drop=True)
        )

        found = len(df_simple)
        m1, m2 = st.columns(2)
        m1.metric("✅ Знайдено ключів", found)
        m2.metric("❌ Без даних", len(keywords) - found)

        COL_CFG = {
            "Обсяг": st.column_config.NumberColumn(format="%d"),
            "CPC ($)": st.column_config.NumberColumn(format="$%.2f"),
            "Конкуренція": st.column_config.NumberColumn(format="%d", help="0–100, Google Ads"),
        }
        st.dataframe(df_simple, use_container_width=True, hide_index=True, column_config=COL_CFG)

        buf = io.BytesIO()
        with pd.ExcelWriter(buf, engine="openpyxl") as writer:
            df_simple.to_excel(writer, index=False, sheet_name="Частота")
            ws = writer.sheets["Частота"]
            for col_cells in ws.columns:
                max_len = max((len(str(cell.value or "")) for cell in col_cells), default=10)
                ws.column_dimensions[col_cells[0].column_letter].width = min(max_len + 4, 50)
        buf.seek(0)
        st.download_button(
            "📥 Завантажити Excel",
            data=buf,
            file_name=f"keyword_volume_{loc['location_code']}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
        )

    # ── MODE: помісячна динаміка ──────────────────────────────────────────
    else:
        rows = []
        for item in raw_results:
            kw = item.get("keyword", "")
            for m in item.get("monthly_searches") or []:
                rows.append({
                    "Ключ": kw,
                    "Рік": m.get("year"),
                    "Місяць": m.get("month"),
                    "Обсяг": m.get("search_volume") or 0,
                })

        df_raw = pd.DataFrame(rows)

        if df_raw.empty:
            st.warning("⚠️ Дані відсутні.")
            st.stop()

        df_raw["Період"] = df_raw.apply(
            lambda r: f"{int(r['Рік'])}-{int(r['Місяць']):02d}", axis=1
        )
        df_pivot = df_raw.pivot_table(
            index="Ключ", columns="Період", values="Обсяг", aggfunc="sum", fill_value=0
        ).reset_index()
        month_cols = sorted([c for c in df_pivot.columns if c != "Ключ"])
        df_pivot = df_pivot[["Ключ"] + month_cols]

        grp = df_raw.groupby("Ключ")["Обсяг"]
        df_summary = pd.DataFrame({
            "Ключ": list(grp.groups.keys()),
            "Середній": grp.mean().round(0).astype(int).values,
            "Макс.": grp.max().values,
            "Мін.": grp.min().values,
        })

        if len(month_cols) >= 2:
            first_col, last_col = month_cols[0], month_cols[-1]
            pivot_idx = df_pivot.set_index("Ключ")
            def _trend(kw):
                first = pivot_idx.at[kw, first_col] if kw in pivot_idx.index else 0
                last = pivot_idx.at[kw, last_col] if kw in pivot_idx.index else 0
                return round((last - first) / first * 100, 1) if first > 0 else None
            df_summary["Тренд %"] = df_summary["Ключ"].map(_trend)

        df_summary = df_summary.sort_values("Середній", ascending=False).reset_index(drop=True)

        found = len(df_pivot)
        m1, m2, m3 = st.columns(3)
        m1.metric("✅ Знайдено ключів", found)
        m2.metric("❌ Без даних", len(keywords) - found)
        m3.metric("📅 Місяців у вибірці", len(month_cols))

        st.caption("Зведена статистика. Повна помісячна розбивка — в Excel.")
        COL_CFG = {
            "Середній": st.column_config.NumberColumn(format="%d"),
            "Макс.": st.column_config.NumberColumn(format="%d"),
            "Мін.": st.column_config.NumberColumn(format="%d"),
            "Тренд %": st.column_config.NumberColumn(format="%.1f%%"),
        }
        st.dataframe(df_summary, use_container_width=True, hide_index=True, column_config=COL_CFG)

        st.divider()
        buf = io.BytesIO()
        with pd.ExcelWriter(buf, engine="openpyxl") as writer:
            df_pivot.to_excel(writer, index=False, sheet_name="По місяцях")
            df_summary.to_excel(writer, index=False, sheet_name="Зведена")
            df_raw[["Ключ", "Рік", "Місяць", "Обсяг"]].to_excel(writer, index=False, sheet_name="Сирі дані")
            for sheet_name in writer.sheets:
                ws = writer.sheets[sheet_name]
                for col_cells in ws.columns:
                    max_len = max((len(str(cell.value or "")) for cell in col_cells), default=10)
                    ws.column_dimensions[col_cells[0].column_letter].width = min(max_len + 4, 50)
        buf.seek(0)
        st.download_button(
            "📥 Завантажити Excel",
            data=buf,
            file_name=f"keyword_volume_{date_from}_{date_to}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
        )
