import hashlib

import pandas as pd
import streamlit as st
import streamlit.components.v1 as components

from content_publisher import (
    build_docx,
    build_docx_zip,
    parse_docx,
    render_html,
    validate_html,
)
from content_publisher.text_parser import parse_plain_text


st.set_page_config(
    page_title="Content Publisher",
    page_icon="📄",
    layout="wide",
)

MAX_FILES = 50
MAX_FILE_SIZE = 10 * 1024 * 1024
MAX_BATCH_SIZE = 100 * 1024 * 1024
ROLE_OPTIONS = ["p", "h2", "h3", "h4", "ul", "ol"]


def _result_key(filename: str, position: int) -> str:
    digest = hashlib.sha1(f"{position}:{filename}".encode()).hexdigest()[:10]
    return f"content_blocks_{digest}"


st.title("📄 Content Publisher")
st.caption("Перетворення DOCX або вставленого тексту в чисті HTML-фрагменти.")

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

input_mode = st.radio(
    "Додайте матеріал",
    ["DOCX-файли", "Вставити текст"],
    horizontal=True,
)

if input_mode == "DOCX-файли":
    uploaded_files = st.file_uploader(
        "Завантажте DOCX-файли",
        type=["docx"],
        accept_multiple_files=True,
        help=f"До {MAX_FILES} файлів, максимум 10 МБ кожен і 100 МБ на всю пачку.",
    )

    if len(uploaded_files) > MAX_FILES:
        st.error(f"Можна обробити не більше {MAX_FILES} файлів за один запуск.")

    oversized = [file.name for file in uploaded_files if file.size > MAX_FILE_SIZE]
    if oversized:
        st.error("Завеликі файли: " + ", ".join(oversized))

    batch_size = sum(file.size for file in uploaded_files)
    batch_too_large = batch_size > MAX_BATCH_SIZE
    if batch_too_large:
        st.error(
            f"Загальний розмір пачки перевищує 100 МБ: "
            f"{batch_size / (1024 * 1024):.1f} МБ."
        )

    process = st.button(
        "⚙️ Обробити документи",
        type="primary",
        use_container_width=True,
        disabled=(
            not uploaded_files
            or len(uploaded_files) > MAX_FILES
            or bool(oversized)
            or batch_too_large
        ),
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
        st.session_state["content_publisher_total_files"] = len(uploaded_files)
        st.session_state["content_publisher_source_mode"] = input_mode
else:
    pasted_text = st.text_area(
        "Скопіюйте та вставте текст",
        height=320,
        placeholder=(
            "Вставте сюди готовий текст. Кожен абзац або заголовок "
            "має бути з нового рядка."
        ),
    )
    st.caption(f"Символів: {len(pasted_text):,}".replace(",", " "))
    st.caption(
        "Форматування та приховані посилання з буфера не переносяться; "
        "для їх збереження використовуйте DOCX."
    )
    process = st.button(
        "⚙️ Перетворити текст",
        type="primary",
        use_container_width=True,
        disabled=not pasted_text.strip(),
    )
    if process:
        st.session_state["content_publisher_results"] = [parse_plain_text(pasted_text)]
        st.session_state["content_publisher_failures"] = []
        st.session_state["content_publisher_total_files"] = 1
        st.session_state["content_publisher_source_mode"] = input_mode

same_mode = st.session_state.get("content_publisher_source_mode") == input_mode
results = st.session_state.get("content_publisher_results", []) if same_mode else []
failures = st.session_state.get("content_publisher_failures", []) if same_mode else []
total_files = st.session_state.get(
    "content_publisher_total_files",
    len(results) + len(failures),
)

if (results or failures) and input_mode == "DOCX-файли":
    st.divider()
    summary_rows = []
    result_statuses = []
    for result in results:
        fragment = render_html(result.blocks, profile)
        validation_errors = validate_html(fragment, profile)
        uncertain_count = sum(1 for block in result.blocks if block.confidence < 0.7)
        problems = [*result.warnings, *validation_errors]
        if validation_errors:
            status = "❌ Помилка HTML"
        elif uncertain_count:
            status = "⚠️ Перевірити"
        elif problems:
            status = "🟡 Зауваження"
        else:
            status = "✅ Готово"
        result_statuses.append(status)
        summary_rows.append(
            {
                "Статус": status,
                "Файл": result.filename,
                "Блоків": len(result.blocks),
                "Заголовків": sum(
                    1 for block in result.blocks if block.role.startswith("h")
                ),
                "Списків": sum(
                    1 for block in result.blocks if block.role in ("ul", "ol")
                ),
                "Посилань": sum(
                    1
                    for block in result.blocks
                    for part in block.parts
                    if part.href
                ),
                "Проблеми": " ".join(problems) if problems else "—",
            }
        )

    for failure in failures:
        filename, _, error = failure.partition(":")
        summary_rows.append(
            {
                "Статус": "❌ Не оброблено",
                "Файл": filename,
                "Блоків": 0,
                "Заголовків": 0,
                "Списків": 0,
                "Посилань": 0,
                "Проблеми": error.strip() or failure,
            }
        )

    ready_count = sum(status == "✅ Готово" for status in result_statuses)
    problem_count = len(summary_rows) - ready_count
    metric1, metric2, metric3, metric4 = st.columns(4)
    metric1.metric("Завантажено", total_files)
    metric2.metric("Оброблено", len(results) + len(failures))
    metric3.metric("Готово", ready_count)
    metric4.metric("З проблемами", problem_count)

    st.markdown("### Зведена по документах")
    st.dataframe(
        pd.DataFrame(summary_rows),
        hide_index=True,
        use_container_width=True,
        height=min(600, 38 * len(summary_rows) + 40),
        column_config={
            "Статус": st.column_config.TextColumn("Статус", width="medium"),
            "Файл": st.column_config.TextColumn("Файл", width="large"),
            "Блоків": st.column_config.NumberColumn("Блоків", width="small"),
            "Заголовків": st.column_config.NumberColumn("Заголовків", width="small"),
            "Списків": st.column_config.NumberColumn("Списків", width="small"),
            "Посилань": st.column_config.NumberColumn("Посилань", width="small"),
            "Проблеми": st.column_config.TextColumn("Проблеми", width="large"),
        },
    )

if results:
    st.divider()
    if input_mode == "DOCX-файли":
        default_index = next(
            (index for index, status in enumerate(result_statuses) if status != "✅ Готово"),
            0,
        )
        selected_position = st.selectbox(
            "Документ для перегляду",
            options=list(range(len(results))),
            index=default_index,
            format_func=lambda index: results[index].filename,
        )
    else:
        selected_position = 0
        st.markdown("### Результат")
    result = results[selected_position]

    for warning in result.warnings:
        st.warning(warning)

    with st.expander("Налаштувати структуру документа"):
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
                key=_result_key(result.filename, selected_position),
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
            html, body {{
                background: #ffffff !important;
                color: #202124 !important;
                font: 16px/1.55 Arial, sans-serif;
                padding: 4px 16px;
            }}
            h2, h3, h4 {{ margin: 1.1em 0 .45em; }}
            p {{ margin: .6em 0; }}
            a {{ color: #0b57d0 !important; }}
        </style>
        {fragment}
        """
        components.html(preview, height=420, scrolling=True)
    with code_tab:
        st.caption("Натисніть значок копіювання у правому верхньому куті блоку.")
        st.code(fragment, language="html", line_numbers=True)

    if input_mode == "DOCX-файли":
        st.download_button(
            "⬇️ Завантажити DOCX з HTML-кодом",
            data=build_docx(fragment),
            file_name=f"{result.filename.rsplit('.', 1)[0]}_HTML.docx",
            mime="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            key=f"download_{_result_key(result.filename, selected_position)}_{profile}",
            disabled=bool(validation_errors),
        )

        st.divider()
        zip_data = build_docx_zip(results, profile)
        st.download_button(
            "📦 Завантажити всі DOCX у ZIP",
            data=zip_data,
            file_name="content_publisher_docx.zip",
            mime="application/zip",
            use_container_width=True,
        )
