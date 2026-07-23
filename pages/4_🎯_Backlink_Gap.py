import io
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

import pandas as pd
import streamlit as st

from ahrefs_api import fetch_referring_domains
from cache import fetch_full_catalog

st.set_page_config(
    page_title="Backlink Gap",
    page_icon="🎯",
    layout="wide",
)

AHREFS_KEY = st.secrets.get("AHREFS_API_KEY", "")
COLLAB_KEY = st.secrets.get("COLLABORATOR_API_KEY", "")


def _norm(raw: str) -> str:
    """Strip protocol, www, paths — return bare domain."""
    d = raw.strip().lower()
    d = re.sub(r"^https?://", "", d)
    d = re.sub(r"^www\.", "", d)
    return d.split("/")[0].split("?")[0]


def _fetch_site_donors(api_key: str, domain: str, limit: int) -> dict[str, dict]:
    """Fetch referring domains. Returns {norm_domain: {dr, traffic, raw_domain}}."""
    try:
        items = fetch_referring_domains(api_key, domain, limit=limit)
    except Exception:
        return {}
    result = {}
    for item in items:
        d = _norm(item.get("domain", ""))
        if d:
            result[d] = {
                "dr": item.get("domain_rating"),
                "traffic": item.get("org_traffic"),
                "raw_domain": item.get("domain", d),
            }
    return result


# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("## ⚙️ Налаштування")
    st.divider()
    donor_limit = st.select_slider(
        "Донорів на сайт",
        options=[500, 1000, 2000, 3000, 5000],
        value=1000,
        help="Топ N донорів за DR, що завантажуються для кожного сайту з Ahrefs.",
    )
    min_dr = st.number_input(
        "Мінімальний DR донора",
        min_value=0,
        max_value=100,
        value=0,
        help="Показувати тільки донорів з DR ≥ цього значення.",
    )
    st.divider()
    st.caption("Competitor Backlinks v1.0")


# ── Main UI ───────────────────────────────────────────────────────────────────
st.title("🎯 Backlink Gap")
st.caption("Пошук донорів, які лінкують на конкурентів, але ще не на тебе.")

if not AHREFS_KEY:
    st.error("❌ Не знайдено AHREFS_API_KEY у секретах. Додай його в Settings → Secrets.")
    st.stop()

# ── Inputs ────────────────────────────────────────────────────────────────────
col_my, col_c1, col_c2, col_c3 = st.columns(4)
with col_my:
    my_site = st.text_input("🏠 Мій сайт", placeholder="mysite.com.ua")
with col_c1:
    comp1 = st.text_input("⚔️ Конкурент 1", placeholder="competitor1.ua")
with col_c2:
    comp2 = st.text_input("⚔️ Конкурент 2", placeholder="competitor2.ua")
with col_c3:
    comp3 = st.text_input("⚔️ Конкурент 3", placeholder="competitor3.ua")

competitors_raw = [c.strip() for c in [comp1, comp2, comp3] if c.strip()]
run_disabled = not my_site.strip() or not competitors_raw

if run_disabled and (my_site.strip() or any(c.strip() for c in [comp1, comp2, comp3])):
    st.caption("ℹ️ Вкажи мій сайт та хоча б одного конкурента.")

run = st.button(
    "🔍 Аналізувати",
    type="primary",
    disabled=run_disabled,
    use_container_width=True,
)

if run:
    my_domain = _norm(my_site.strip())
    comp_domains = [_norm(c) for c in competitors_raw]
    all_targets = [my_domain] + comp_domains

    # ── Fetch donors from Ahrefs ──────────────────────────────────────────
    donors_by_site: dict[str, dict] = {}
    progress = st.progress(0.0, text="Завантажуємо донорів з Ahrefs…")
    fetch_errors = []

    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = {
            executor.submit(_fetch_site_donors, AHREFS_KEY, t, donor_limit): t
            for t in all_targets
        }
        done_count = 0
        for future in as_completed(futures):
            target = futures[future]
            try:
                donors_by_site[target] = future.result()
            except Exception as e:
                donors_by_site[target] = {}
                fetch_errors.append(f"{target}: {e}")
            done_count += 1
            progress.progress(
                done_count / len(all_targets),
                text=f"Завантажено {done_count}/{len(all_targets)} сайтів…",
            )

    progress.empty()
    for err in fetch_errors:
        st.warning(f"⚠️ Помилка при завантаженні {err}")

    my_donors = donors_by_site.get(my_domain, {})

    # ── Apply min DR filter ───────────────────────────────────────────────
    if min_dr > 0:
        my_donors = {d: v for d, v in my_donors.items() if (v.get("dr") or 0) >= min_dr}
        for cd in comp_domains:
            donors_by_site[cd] = {
                d: v for d, v in donors_by_site.get(cd, {}).items()
                if (v.get("dr") or 0) >= min_dr
            }

    # Build combined competitor donor pool (highest DR wins for duplicates)
    all_comp_donors: dict[str, dict] = {}
    for cd in comp_domains:
        for d, v in donors_by_site.get(cd, {}).items():
            existing_dr = (all_comp_donors.get(d) or {}).get("dr") or 0
            if d not in all_comp_donors or (v.get("dr") or 0) > existing_dr:
                all_comp_donors[d] = v

    my_set = set(my_donors.keys())
    comp_set = set(all_comp_donors.keys())
    gap_set = comp_set - my_set
    my_only_set = my_set - comp_set
    shared_set = my_set & comp_set

    # ── Collaborator enrichment ───────────────────────────────────────────
    collab_lookup: dict = {}
    if COLLAB_KEY:
        with st.spinner("Звіряємо з каталогом Collaborator…"):
            try:
                df_catalog = fetch_full_catalog(COLLAB_KEY)
                collab_lookup = {
                    _norm(row["domain"]): row
                    for _, row in df_catalog.iterrows()
                    if row.get("domain")
                }
            except Exception as e:
                st.warning(f"⚠️ Помилка завантаження Collaborator: {e}")

    def _collab(domain: str) -> tuple:
        info = collab_lookup.get(domain, {})
        if not info:
            return False, None, None, ""
        price = int(info["price"]) if info.get("price") else None
        price_w = int(info["price_writing"]) if info.get("price_writing") else None
        return True, price, price_w, info.get("collaborator_url", "")

    def _which_comps(domain: str) -> str:
        return ", ".join(cd for cd in comp_domains if domain in donors_by_site.get(cd, {}))

    def _comp_count(domain: str) -> int:
        return sum(1 for cd in comp_domains if domain in donors_by_site.get(cd, {}))

    # ── Build DataFrames ──────────────────────────────────────────────────
    def _make_df(domain_set: set, donor_source: dict, with_comp_cols: bool = False) -> pd.DataFrame:
        rows = []
        for d in domain_set:
            info = donor_source.get(d, {})
            in_c, price, price_w, c_url = _collab(d)
            row = {
                "Домен": info.get("raw_domain", d),
                "DR": info.get("dr"),
                "Органічний трафік": info.get("traffic"),
                "В Collaborator": "Так" if in_c else "Ні",
                "Ціна публікації (грн)": price,
                "Ціна написання (грн)": price_w,
                "Collaborator": c_url,
            }
            if with_comp_cols:
                row["К-сть конкурентів"] = _comp_count(d)
                row["Конкуренти"] = _which_comps(d)
            rows.append(row)
        if not rows:
            return pd.DataFrame()
        return pd.DataFrame(rows)

    # Gap
    df_gap = _make_df(gap_set, all_comp_donors, with_comp_cols=True)
    if not df_gap.empty:
        df_gap = df_gap.sort_values(
            ["К-сть конкурентів", "DR"], ascending=[False, False]
        ).reset_index(drop=True)

    # My donors
    my_rows = []
    for d in my_set:
        info = my_donors.get(d, {})
        in_c, price, price_w, c_url = _collab(d)
        in_shared = d in shared_set
        my_rows.append({
            "Домен": info.get("raw_domain", d),
            "DR": info.get("dr"),
            "Органічний трафік": info.get("traffic"),
            "Є у конкурентів": "Так" if in_shared else "Ні",
            "Конкуренти": _which_comps(d) if in_shared else "",
            "В Collaborator": "Так" if in_c else "Ні",
            "Ціна публікації (грн)": price,
            "Ціна написання (грн)": price_w,
            "Collaborator": c_url,
        })
    df_my = pd.DataFrame(my_rows)
    if not df_my.empty:
        df_my = df_my.sort_values("DR", ascending=False).reset_index(drop=True)

    # Shared
    df_shared = _make_df(shared_set, my_donors, with_comp_cols=True)
    if not df_shared.empty:
        df_shared = df_shared.sort_values(
            ["К-сть конкурентів", "DR"], ascending=[False, False]
        ).reset_index(drop=True)

    # ── Summary metrics ───────────────────────────────────────────────────
    st.divider()
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("🔗 Мої донори", len(my_set))
    c2.metric("🎯 Gap донори", len(gap_set))
    c3.metric("🔄 Спільні", len(shared_set))
    c4.metric("⭐ Тільки у мене", len(my_only_set))

    # ── Column config ─────────────────────────────────────────────────────
    COL_CFG = {
        "DR": st.column_config.NumberColumn(format="%d"),
        "Органічний трафік": st.column_config.NumberColumn(format="%d"),
        "Ціна публікації (грн)": st.column_config.NumberColumn(format="%d"),
        "Ціна написання (грн)": st.column_config.NumberColumn(format="%d"),
        "Collaborator": st.column_config.LinkColumn(
            "Collaborator",
            display_text="Відкрити",
            help="Відкрити на Collaborator.pro",
        ),
    }

    tab_gap, tab_my, tab_shared = st.tabs([
        f"🎯 Gap — {len(gap_set)}",
        f"🔗 Мої донори — {len(my_set)}",
        f"🔄 Спільні — {len(shared_set)}",
    ])

    with tab_gap:
        st.caption("Сайти, що дають посилання конкурентам, але ще не тобі. Відсортовано: к-сть конкурентів → DR.")
        if df_gap.empty:
            st.info("ℹ️ Gap-донорів не знайдено.")
        else:
            gap_cols = [
                "Домен", "DR", "Органічний трафік", "К-сть конкурентів", "Конкуренти",
                "В Collaborator", "Ціна публікації (грн)", "Ціна написання (грн)", "Collaborator",
            ]
            st.dataframe(
                df_gap[[c for c in gap_cols if c in df_gap.columns]],
                use_container_width=True,
                hide_index=True,
                column_config=COL_CFG,
            )

    with tab_my:
        st.caption("Всі донори твого сайту. Позначено, чи є вони також у конкурентів.")
        if df_my.empty:
            st.info("ℹ️ Донорів не знайдено.")
        else:
            my_cols = [
                "Домен", "DR", "Органічний трафік", "Є у конкурентів", "Конкуренти",
                "В Collaborator", "Ціна публікації (грн)", "Ціна написання (грн)", "Collaborator",
            ]
            st.dataframe(
                df_my[[c for c in my_cols if c in df_my.columns]],
                use_container_width=True,
                hide_index=True,
                column_config=COL_CFG,
            )

    with tab_shared:
        st.caption("Донори, що лінкують і на тебе, і на конкурентів.")
        if df_shared.empty:
            st.info("ℹ️ Спільних донорів немає.")
        else:
            sh_cols = [
                "Домен", "DR", "Органічний трафік", "К-сть конкурентів", "Конкуренти",
                "В Collaborator", "Ціна публікації (грн)", "Ціна написання (грн)", "Collaborator",
            ]
            st.dataframe(
                df_shared[[c for c in sh_cols if c in df_shared.columns]],
                use_container_width=True,
                hide_index=True,
                column_config=COL_CFG,
            )

    # ── Excel export ──────────────────────────────────────────────────────
    st.divider()
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        if not df_gap.empty:
            df_gap.to_excel(writer, index=False, sheet_name="Gap (нові донори)")
        if not df_my.empty:
            df_my.to_excel(writer, index=False, sheet_name="Мої донори")
        if not df_shared.empty:
            df_shared.to_excel(writer, index=False, sheet_name="Спільні")
    buf.seek(0)
    st.download_button(
        "📥 Завантажити Excel (всі листи)",
        data=buf,
        file_name=f"competitor_backlinks_{my_domain}_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        use_container_width=True,
    )
