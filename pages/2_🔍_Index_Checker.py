import asyncio
import io
import base64
import re
from urllib.parse import urlparse

import pandas as pd
import requests
import streamlit as st

st.set_page_config(page_title="Index Checker", page_icon="🔍", layout="wide")

# ── URL validation ─────────────────────────────────────────────────────────────
_PRIVATE_HOST = re.compile(
    r'^(localhost|127\.|10\.|192\.168\.|172\.(1[6-9]|2\d|3[01])\.|169\.254\.|metadata\.google\.internal)',
    re.IGNORECASE,
)

def _is_valid_url(url: str) -> bool:
    if len(url) > 2000:
        return False
    try:
        parsed = urlparse(url)
        if parsed.scheme not in ("http", "https"):
            return False
        host = parsed.hostname or ""
        if not host or _PRIVATE_HOST.match(host):
            return False
        return True
    except Exception:
        return False

def _split_raw_urls(raw: str) -> list:
    urls = []
    for line in raw.splitlines():
        line = line.strip()
        if not line:
            continue
        parts = re.split(r'(?=https?://)', line)
        for part in parts:
            part = part.strip()
            if part:
                urls.append(part)
    return urls

def _sanitize_excel_cell(value):
    if isinstance(value, str) and value.startswith(("=", "+", "-", "@")):
        return "'" + value
    return value

from checker import DataForSEOChecker, SerpAPIChecker
from page_checker import check_pages

st.title("🔍 URL Indexing Checker")
st.caption("Перевірка індексації, HTTP статусу, noindex та nofollow")

# ── Session state init ────────────────────────────────────────────────────────
for key, default in [
    ("api_login", ""), ("api_password", ""), ("api_key", ""),
    ("verified", False), ("running", False),
    ("df_results", None), ("summary", None),
]:
    if key not in st.session_state:
        st.session_state[key] = default

# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.header("Налаштування API")

    provider = st.selectbox(
        "Провайдер",
        ["DataForSEO", "SerpAPI"],
        help="DataForSEO — ~$0.002/запит. SerpAPI — від $50/міс за 5 000 запитів.",
    )

    if provider == "DataForSEO":
        secret_login    = st.secrets.get("DATAFORSEO_LOGIN", "").encode("ascii", "ignore").decode().strip()
        secret_password = st.secrets.get("DATAFORSEO_PASSWORD", "").encode("ascii", "ignore").decode().strip()

        if secret_login and secret_password:
            api_login    = secret_login
            api_password = secret_password
            st.success("API credentials з секретів")
        else:
            api_login    = st.text_input("Login",    value=st.session_state.api_login,    type="password")
            api_password = st.text_input("Password", value=st.session_state.api_password, type="password")
        credentials_ok = bool(api_login and api_password)

        if st.button("Тест з'єднання", disabled=not credentials_ok):
            creds = base64.b64encode(f"{api_login}:{api_password}".encode()).decode()
            try:
                resp = requests.get(
                    "https://api.dataforseo.com/v3/appendix/user_data",
                    headers={"Authorization": f"Basic {creds}"},
                    timeout=15,
                )
                data = resp.json()
                if data.get("status_code") == 20000:
                    st.session_state.api_login    = api_login
                    st.session_state.api_password = api_password
                    st.session_state.verified     = True
                    st.success("З'єднання OK!")
                else:
                    st.session_state.verified = False
                    st.error(f"Помилка {data.get('status_code')}: {data.get('status_message')}")
            except Exception as e:
                st.error(f"Помилка: {e}")

        if st.session_state.verified:
            st.success("Credentials збережено в сесії")

    else:
        api_key = st.text_input("API Key", value=st.session_state.api_key, type="password")
        credentials_ok = bool(api_key)
        if api_key:
            st.session_state.api_key = api_key

    st.divider()
    check_page_meta = st.toggle("Перевіряти HTTP / Noindex / Nofollow", value=True)
    target_domain = st.text_input(
        "Ваш домен (для nofollow)",
        placeholder="mysite.com",
        help="Парсер знайде всі посилання на ваш домен і перевірить rel атрибут.",
    )
    if check_page_meta and not target_domain.strip():
        st.caption("ℹ️ Без домену колонка «Тип посилання» буде порожньою.")

    st.divider()
    st.caption("dataforseo.com" if provider == "DataForSEO" else "serpapi.com")

URL_LIMIT = 500

# ── Resolve active credentials ────────────────────────────────────────────────
if provider == "DataForSEO":
    active_login    = api_login
    active_password = api_password
    credentials_ok  = bool(active_login and active_password)
else:
    active_key     = api_key
    credentials_ok = bool(active_key)

# ── Input ─────────────────────────────────────────────────────────────────────
st.subheader("Список URL для перевірки")
st.caption(f"Максимум {URL_LIMIT} URL за один запуск.")

input_method = st.radio("Спосіб введення", ["Текстове поле", "CSV / TXT файл"], horizontal=True)
urls = []

if input_method == "Текстове поле":
    raw = st.text_area(
        "По одному URL на рядок", height=250,
        placeholder="https://donor-site.com/page\nhttps://another-donor.com/article",
    )
    if raw:
        urls = _split_raw_urls(raw)
else:
    uploaded = st.file_uploader("CSV або TXT файл", type=["csv", "txt"])
    if uploaded:
        if uploaded.size > 5 * 1024 * 1024:
            st.error("Файл завеликий. Максимум — 5MB.")
        elif uploaded.name.endswith(".csv"):
            df_upload = pd.read_csv(uploaded)
            all_cells = df_upload.values.flatten()
            urls = [
                str(cell).strip() for cell in all_cells
                if pd.notna(cell) and str(cell).strip().startswith(("http://", "https://"))
            ]
            if not urls:
                st.warning("У CSV не знайдено жодного URL що починається з http:// або https://")
            elif len(urls) > URL_LIMIT:
                st.error(f"CSV містить {len(urls)} URL. Максимум — {URL_LIMIT}.")
                urls = []
        else:
            content = uploaded.read().decode("utf-8")
            urls = _split_raw_urls(content)
            if len(urls) > URL_LIMIT:
                st.error(f"TXT містить {len(urls)} URL. Максимум — {URL_LIMIT}.")
                urls = []

if urls:
    unique_urls = list(dict.fromkeys(urls))
    removed = len(urls) - len(unique_urls)
    if removed > 0:
        st.info(f"Знайдено {removed} дублікатів — видалено. Залишилось {len(unique_urls)} унікальних URL.")
    urls = unique_urls

if urls:
    valid_urls = [u for u in urls if _is_valid_url(u)]
    invalid_count = len(urls) - len(valid_urls)
    if invalid_count > 0:
        st.warning(f"Відфільтровано {invalid_count} рядків: не http/https або приватні адреси.")
    urls = valid_urls

if urls:
    if len(urls) > URL_LIMIT:
        st.error(f"Занадто багато URL: {len(urls)}. Максимум — {URL_LIMIT}.")
        urls = []
    else:
        col_a, col_b = st.columns(2)
        col_a.metric("URL до перевірки", len(urls))
        if provider == "DataForSEO":
            col_b.metric("Орієнтовна вартість", f"~${len(urls) * 0.002:.2f}")
        else:
            col_b.metric("Запитів", len(urls))

st.divider()

if not credentials_ok and urls:
    st.warning("Введіть API credentials у бічній панелі.")

def _start_running():
    st.session_state.running = True

if st.button(
    "Перевірити",
    type="primary",
    disabled=(not urls or not credentials_ok or st.session_state.running),
    on_click=_start_running,
    use_container_width=True,
):
    if provider == "DataForSEO":
        try:
            creds = base64.b64encode(f"{active_login}:{active_password}".encode()).decode()
            resp_balance = requests.get(
                "https://api.dataforseo.com/v3/appendix/user_data",
                headers={"Authorization": f"Basic {creds}"},
                timeout=10,
            )
            balance_data = resp_balance.json()
            balance = (
                ((balance_data.get("tasks") or [{}])[0].get("result") or [{}])[0]
                .get("money", {}).get("balance", None)
            )
            estimated_cost = len(urls) * 0.002
            if balance is not None:
                if balance < estimated_cost:
                    st.warning(f"Увага: баланс ${balance:.2f}, а запуск коштує ~${estimated_cost:.2f}.")
                else:
                    st.caption(f"Баланс: ${balance:.2f} / Вартість запуску: ~${estimated_cost:.2f}")
        except Exception:
            pass

    st.write("**Крок 1/2:** Перевірка індексації...")
    progress_bar       = st.progress(0.0)
    status_placeholder = st.empty()

    def on_progress(done, total):
        progress_bar.progress(done / total)
        status_placeholder.text(f"Індексація: {done} / {total}...")

    if provider == "DataForSEO":
        checker = DataForSEOChecker(active_login, active_password, 5)
    else:
        checker = SerpAPIChecker(active_key, 5)

    try:
        index_results = asyncio.run(checker.check_urls(urls, on_progress))
    except RuntimeError:
        import nest_asyncio
        nest_asyncio.apply()
        loop = asyncio.get_event_loop()
        index_results = loop.run_until_complete(checker.check_urls(urls, on_progress))

    progress_bar.progress(1.0)
    status_placeholder.empty()

    page_results_map = {}
    if check_page_meta:
        st.write("**Крок 2/2:** Перевірка HTTP / Noindex / Nofollow...")
        progress_bar2       = st.progress(0.0)
        status_placeholder2 = st.empty()

        def on_progress2(done, total):
            progress_bar2.progress(done / total)
            status_placeholder2.text(f"Сторінки: {done} / {total}...")

        try:
            page_results = asyncio.run(check_pages(urls, target_domain.strip(), 5, on_progress2))
        except RuntimeError:
            import nest_asyncio
            nest_asyncio.apply()
            loop = asyncio.get_event_loop()
            page_results = loop.run_until_complete(check_pages(urls, target_domain.strip(), 5, on_progress2))

        progress_bar2.progress(1.0)
        status_placeholder2.empty()
        page_results_map = {r.url: r for r in page_results}

    def index_label(r):
        if r.error:
            return f"Помилка: {r.error}"
        return "в індексі" if r.indexed else "не в індексі"

    rows = []
    for r in index_results:
        row = {"URL": r.url, "Індексація": index_label(r)}
        if check_page_meta and r.url in page_results_map:
            pr = page_results_map[r.url]
            row["HTTP статус"]    = str(pr.http_status) if pr.http_status else f"Помилка: {pr.error}"
            row["Noindex"]        = "так" if pr.noindex else ("ні" if pr.noindex is False else "—")
            row["Тип посилання"]  = pr.nofollow or "—"
        rows.append(row)

    st.session_state.df_results = pd.DataFrame(rows)
    st.session_state.summary = {
        "indexed":     sum(1 for r in index_results if r.indexed is True),
        "not_indexed": sum(1 for r in index_results if r.indexed is False),
        "errors":      sum(1 for r in index_results if r.error),
    }
    st.session_state.running = False

# ── Results ───────────────────────────────────────────────────────────────────
if st.session_state.df_results is not None:
    s = st.session_state.summary
    m1, m2, m3 = st.columns(3)
    m1.metric("В індексі",    s["indexed"])
    m2.metric("Не в індексі", s["not_indexed"])
    m3.metric("Помилки",      s["errors"])

    df_results = st.session_state.df_results
    filter_opt = st.radio("Показати", ["Всі", "в індексі", "не в індексі", "Помилки"], horizontal=True)

    if filter_opt == "в індексі":
        display_df = df_results[df_results["Індексація"] == "в індексі"]
    elif filter_opt == "не в індексі":
        display_df = df_results[df_results["Індексація"] == "не в індексі"]
    elif filter_opt == "Помилки":
        display_df = df_results[df_results["Індексація"].str.startswith("Помилка")]
    else:
        display_df = df_results

    st.dataframe(display_df, use_container_width=True, hide_index=True, height=400)

    excel_buffer = io.BytesIO()
    df_export = df_results.apply(lambda col: col.map(_sanitize_excel_cell))
    with pd.ExcelWriter(excel_buffer, engine="openpyxl") as writer:
        df_export.to_excel(writer, index=False, sheet_name="Indexing")
        ws = writer.sheets["Indexing"]
        for col_cells in ws.columns:
            max_len = max(len(str(cell.value or "")) for cell in col_cells)
            ws.column_dimensions[col_cells[0].column_letter].width = min(max_len + 4, 80)

    st.download_button(
        label="Скачати Excel",
        data=excel_buffer.getvalue(),
        file_name="indexing_results.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        use_container_width=True,
    )
