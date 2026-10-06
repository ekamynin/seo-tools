import hashlib

import pandas as pd
import streamlit as st
import streamlit.components.v1 as components

from content_publisher import build_zip, parse_docx, render_html, validate_html


st.set_page_config(
    page_title="Content Publisher",
    page_icon="📄",
    layout="wide",
)

MAX_FILES = 50
MAX_FILE_SIZE = 25 * 1024 * 1024
ROLE_OPTIONS = ["p", "h2", "h3", "h4", "ul", "ol"]


def _result_key(filename: str, position: int) -> str:
    digest = hashlib.sha1(f"{position}:{filename}".encode()).hexdigest()[:10]
    return f"content_blocks_{digest}"


st.title("📄 Content Publisher")
st.caption("Пакетне перетворення DOCX у чисті HTML-фрагменти без зайвих тегів.")

with st.sidebar:
    st.markdown("## ⚙️ Налаштування")
    st.divider()
    leroy_mode = st.checkbox(
        "Текст для Leroy Merlin",
        help=(
            "Додає клієнтські атрибути до посилань і списків, зелений колір "
            "анкорів та <br><br> наприкінці абзаців."
        ),
    )
    st.caption("Жирне форматування завжди видаляється.")
    st.divider()
    st.caption("Content Publisher v0.1")

profile = "leroy_merlin" if leroy_mode else "default"

uploaded_files = st.file_uploader(
    "Завантажте DOCX-файли",
    type=["docx"],
    accept_multiple_files=True,
    help=f"До {MAX_FILES} файлів, максимум 25 МБ кожен.",
)

if len(uploaded_files) > MAX_FILES:
    st.error(f"Можна обробити не більше {MAX_FILES} файлів за один запуск.")

oversized = [file.name for file in uploaded_files if file.size > MAX_FILE_SIZE]
if oversized:
    st.error("Завеликі файли: " + ", ".join(oversized))

process = st.button(
    "⚙️ Обробити документи",
    type="primary",
    use_container_width=True,
    disabled=not uploaded_files or len(uploaded_files) > MAX_FILES or bool(oversized),
)

if process:
    results = []
    failures = []
    progress = st.progress(0.0, text="Читаємо документи…")
    for position, uploaded in enumerate(uploaded_files):
        try:
            results.append(parse_docx(uploaded.getvalue(), uploaded.name))
        except Exception as exc:
            failures.append(f"{uploaded.name}: {exc}")
        progress.progress(
            (position + 1) / len(uploaded_files),
            text=f"Оброблено {position + 1}/{len(uploaded_files)}",
        )
    progress.empty()
    st.session_state["content_publisher_results"] = results
    st.session_state["content_publisher_failures"] = failures

results = st.session_state.get("content_publisher_results", [])
failures = st.session_state.get("content_publisher_failures", [])

for failure in failures:
    st.error(f"❌ {failure}")

if results:
    st.divider()
    warning_count = sum(len(result.warnings) for result in results)
    metric1, metric2, metric3 = st.columns(3)
    metric1.metric("Документів", len(results))
    metric2.metric("Блоків", sum(len(result.blocks) for result in results))
    metric3.metric("Попереджень", warning_count)

    for position, result in enumerate(results):
        with st.expander(f"📄 {result.filename}", expanded=len(results) == 1):
            for warning in result.warnings:
                st.warning(warning)

            rows = [
                {
                    "#": block.index,
                    "Тип": block.role,
                    "Текст": block.text,
                    "Упевненість": f"{block.confidence:.0%}",
                    "Чому": block.reason,
                }
                for block in result.blocks
            ]
            edited = st.data_editor(
                pd.DataFrame(rows),
                key=_result_key(result.filename, position),
                hide_index=True,
                use_container_width=True,
                disabled=["#", "Текст", "Упевненість", "Чому"],
                column_config={
                    "#": st.column_config.NumberColumn("#", width="small"),
                    "Тип": st.column_config.SelectboxColumn(
                        "Тип",
                        options=ROLE_OPTIONS,
                        required=True,
                        width="small",
                    ),
                    "Текст": st.column_config.TextColumn("Текст", width="large"),
                    "Упевненість": st.column_config.TextColumn("Упевненість", width="small"),
                    "Чому": st.column_config.TextColumn("Чому", width="medium"),
                },
            )

            for block, role in zip(result.blocks, edited["Тип"].tolist()):
                block.role = role if role in ROLE_OPTIONS else "p"

            fragment = render_html(result.blocks, profile)
            validation_errors = validate_html(fragment, profile)
            for error in validation_errors:
                st.error(f"Перевірка HTML: {error}")

            preview_tab, code_tab = st.tabs(["Перегляд", "HTML"])
            with preview_tab:
                preview = f"""
                <style>
                    body {{ font: 16px/1.55 Arial, sans-serif; color: #202124; padding: 4px 16px; }}
                    h2, h3, h4 {{ margin: 1.1em 0 .45em; }}
                    p {{ margin: .6em 0; }}
                </style>
                {fragment}
                """
                components.html(preview, height=360, scrolling=True)
            with code_tab:
                st.code(fragment, language="html", line_numbers=True)

            st.download_button(
                "⬇️ Завантажити HTML",
                data=fragment.encode("utf-8"),
                file_name=f"{result.filename.rsplit('.', 1)[0]}.html",
                mime="text/html",
                key=f"download_{_result_key(result.filename, position)}_{profile}",
                disabled=bool(validation_errors),
            )

    st.divider()
    zip_data = build_zip(results, profile)
    st.download_button(
        "📦 Завантажити всі HTML у ZIP",
        data=zip_data,
        file_name="content_publisher_html.zip",
        mime="application/zip",
        use_container_width=True,
    )
