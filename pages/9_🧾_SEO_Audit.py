from pathlib import Path

import streamlit as st
import streamlit.components.v1 as components


st.set_page_config(page_title="Technical Audit", page_icon="🧾", layout="wide")
st.title("Technical Audit")

assets = Path(__file__).resolve().parents[1] / "seo_audit"
template = (assets / "checklist.html").read_text(encoding="utf-8")
checks = (assets / "checklist.json").read_text(encoding="utf-8").replace("<", "\\u003c")
components.html(template.replace("{{CHECKLIST_DATA}}", checks), height=820, scrolling=True)
