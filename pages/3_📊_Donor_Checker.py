import io
import re
from datetime import datetime

import pandas as pd
import streamlit as st

from ahrefs_api import enrich_with_ahrefs
from cache import fetch_full_catalog
from link_builder import CATEGORY_TRANSLATIONS

st.set_page_config(
    page_title="Перевірка майданчиків",
    page_icon="📊",
    layout="wide",
)

COLLAB_KEY = st.secrets.get("COLLABORATOR_API_KEY", "")
AHREFS_KEY  = st.secrets.get("AHREFS_API_KEY", "")


def _normalize(raw: str) -> str:
    d = raw.strip().lower()
    d = re.sub(r'^[\s"\'«»„""\(\[\{]+', "", d)
    d = re.sub(r'[\s"\'«»„""\)\]\}]+$', "", d)
    d = re.sub(r"^https?://", "", d)
    d = re.sub(r"^www\.", "", d)
    d = d.split("/")[0].split("?")[0]
    return d


def _translate_categories(raw: str) -> str:
    parts = [p.strip() for p in raw.split(",") if p.strip()]
    seen, result = set(), []
    for p in parts:
        ua = CATEGORY_TRANSLATIONS.get(p, p)
        if ua not in seen:
            seen.add(ua)
            result.append(ua)
    return ", ".join(result)


def _parse_input(text: str) -> list[str]:
    seen, result = set(), []
    for raw in re.split(r"[\n,]+", text):
        d = _normalize(raw)
        if d and "." in d and d not in seen:
            seen.add(d)
            result.append(d)
    return result


# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("## Налаштування")
    st.divider()
    if "catalog_loaded_at" in st.session_state:
        st.success(f"✅ {st.session_state['catalog_size']:,} майданчиків")
        st.caption(f"Оновлено: {st.session_state['catalog_loaded_at']}")
    if st.button("Оновити каталог", use_container_width=True,
                 help="Примусово оновити каталог Collaborator (~1 хв)"):
        st.cache_data.clear()
        for key in ("catalog_loaded_at", "catalog_size"):
            st.session_state.pop(key, None)
        st.rerun()
    st.divider()
    st.caption("Donor Checker v1.0")


# ── Auto-load catalog ─────────────────────────────────────────────────────────
if not COLLAB_KEY:
    st.error("Не знайдено COLLABORATOR_API_KEY у секретах.")
    st.stop()

if "catalog_loaded_at" not in st.session_state:
    with st.spinner("Завантажуємо каталог майданчиків..."):
        try:
            df_catalog = fetch_full_catalog(COLLAB_KEY)
            st.session_state["catalog_loaded_at"] = datetime.now().strftime("%d.%m.%Y %H:%M")
            st.session_state["catalog_size"] = len(df_catalog)
            st.rerun()
        except Exception as e:
            st.error(f"Помилка завантаження Collaborator: {e}")
            st.stop()
else:
    df_catalog = fetch_full_catalog(COLLAB_KEY)

collab_lookup: dict = {
    _normalize(row["domain"]): row
    for _, row in df_catalog.iterrows()
}


# ── Main UI ───────────────────────────────────────────────────────────────────
st.title("Перевірка майданчиків")
st.caption("Закидай список URL — отримаєш ціну з Collaborator, DR і трафік з Ahrefs для кожного донора.")

urls_input = st.text_area(
    "Список URL або доменів (по одному на рядок або через кому)",
    placeholder="https://example.com.ua\nblog.site.ua\nsite2.ua",
    height=180,
)

run_btn = st.button("Перевірити", type="primary")

if run_btn:
    if not urls_input.strip():
        st.warning("Введи хоча б один URL або домен.")
        st.stop()

    domains = _parse_input(urls_input)
    if not domains:
        st.warning("Не вдалось розпізнати жодного домену. Перевір введені дані.")
        st.stop()

    matched   = [d for d in domains if d in collab_lookup]
    not_found = [d for d in domains if d not in collab_lookup]

    # ── Ahrefs enrichment for ALL input domains ────────────────────────────
    ahrefs_data: dict = {}
    if AHREFS_KEY:
        with st.spinner(f"Отримуємо дані Ahrefs для {len(domains)} доменів..."):
            try:
                ahrefs_data = enrich_with_ahrefs(AHREFS_KEY, domains)
            except Exception:
                pass

    # ── Build results table ────────────────────────────────────────────────
    rows = []
    for d in domains:
        ah = ahrefs_data.get(d, {})

        if d in collab_lookup:
            site = collab_lookup[d]
            dr_val = ah.get("dr") if ah.get("dr") is not None else site["dr"]
            tr_val = (
                ah.get("org_traffic")
                if ah.get("org_traffic") is not None
                else site["organic_traffic"]
            )
            price   = site.get("price")
            price_w = site.get("price_writing")
            rows.append({
                "Домен": d,
                "В Collaborator": "Так",
                "Ціна публікації (грн)": int(price)   if price   and pd.notna(price)   else None,
                "Ціна написання (грн)":  int(price_w) if price_w and pd.notna(price_w) else None,
                "Тематика": _translate_categories(site.get("categories", "")),
                "DR": int(dr_val) if dr_val is not None else None,
                "Органічний трафік": int(tr_val) if tr_val is not None else None,
                "Стан трафіку": ah.get("traffic_label") or "OK",
                "Collaborator": site.get("collaborator_url", ""),
            })
        else:
            dr_val = ah.get("dr")
            tr_val = ah.get("org_traffic")
            rows.append({
                "Домен": d,
                "В Collaborator": "Ні",
                "Ціна публікації (грн)": None,
                "Ціна написання (грн)":  None,
                "Тематика": "",
                "DR": int(dr_val) if dr_val is not None else None,
                "Органічний трафік": int(tr_val) if tr_val is not None else None,
                "Стан трафіку": ah.get("traffic_label") or ("OK" if ah else "—"),
                "Collaborator": "",
            })

    df_result = pd.DataFrame(rows)

    # ── Summary ───────────────────────────────────────────────────────────
    c1, c2, c3 = st.columns(3)
    c1.metric("Перевірено доменів", len(domains))
    c2.metric("Знайдено в Collaborator", len(matched))
    c3.metric("Не знайдено", len(not_found))

    # ── Table ─────────────────────────────────────────────────────────────
    st.dataframe(
        df_result,
        use_container_width=True,
        hide_index=True,
        column_config={
            "Ціна публікації (грн)": st.column_config.NumberColumn(format="%d"),
            "Ціна написання (грн)":  st.column_config.NumberColumn(format="%d"),
            "DR":                    st.column_config.NumberColumn(format="%d"),
            "Органічний трафік":     st.column_config.NumberColumn(format="%d"),
            "Collaborator": st.column_config.LinkColumn(
                "Collaborator",
                display_text="Відкрити",
                help="Відкрити майданчик на Collaborator.pro",
            ),
        },
    )

    if not_found:
        with st.expander(f"Не знайдено в Collaborator ({len(not_found)})"):
            for d in not_found:
                st.markdown(f"- `{d}`")

    # ── Excel export ──────────────────────────────────────────────────────
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        df_result.to_excel(writer, index=False, sheet_name="Перевірка майданчиків")
    buf.seek(0)
    st.download_button(
        "Завантажити Excel",
        data=buf,
        file_name=f"donor_check_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
