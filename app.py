from datetime import datetime
import streamlit as st
from cache import fetch_full_catalog

st.set_page_config(
    page_title="SEO Tools",
    page_icon="🛠️",
    layout="wide",
)

COLLAB_KEY = st.secrets.get("COLLABORATOR_API_KEY", "")

# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("## Каталог майданчиків")
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

st.title("🛠️ SEO Tools")
st.caption("Внутрішні інструменти для SEO-команди")
st.divider()

# ── Pre-warm catalog ──────────────────────────────────────────────────────────
if COLLAB_KEY and "catalog_loaded_at" not in st.session_state:
    with st.spinner("Завантажуємо каталог майданчиків..."):
        try:
            df = fetch_full_catalog(COLLAB_KEY)
            st.session_state["catalog_loaded_at"] = datetime.now().strftime("%d.%m.%Y %H:%M")
            st.session_state["catalog_size"] = len(df)
            st.rerun()
        except Exception:
            pass

col1, col2, col3 = st.columns(3, gap="large")

with col1:
    st.markdown("### 🔗 Link Builder")
    st.markdown(
        "Підбір донорів для лінкбілдингу з бази Collaborator.pro. "
        "Фільтрація за тематикою, DR, трафіком і бюджетом. "
        "Перевірка якості через Ahrefs."
    )
    st.page_link("pages/1_🔗_Link_Builder.py", label="Відкрити Link Builder", icon="🔗")

with col2:
    st.markdown("### 🔍 Index Checker")
    st.markdown(
        "Масова перевірка індексації URL через DataForSEO або SerpAPI. "
        "HTTP статус, noindex, nofollow. "
        "Підтримує до 500 URL за запуск, експорт в Excel."
    )
    st.page_link("pages/2_🔍_Index_Checker.py", label="Відкрити Index Checker", icon="🔍")

with col3:
    st.markdown("### 📊 Donor Checker")
    st.markdown(
        "Масова перевірка списку донорів. "
        "Ціна публікації та написання з Collaborator.pro. "
        "DR і органічний трафік через Ahrefs."
    )
    st.page_link("pages/3_📊_Donor_Checker.py", label="Відкрити Donor Checker", icon="📊")
