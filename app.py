import streamlit as st

st.set_page_config(
    page_title="SEO Tools",
    page_icon="🛠️",
    layout="wide",
)

st.title("🛠️ SEO Tools")
st.caption("Внутрішні інструменти для SEO-команди")
st.divider()

col1, col2 = st.columns(2, gap="large")

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
