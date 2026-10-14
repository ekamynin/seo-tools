import html
import re

import streamlit as st

from robots_checker import (
    RobotsFetchError,
    analyze_robots,
    fetch_robots,
    normalize_robots_url,
    test_url,
)


st.set_page_config(
    page_title="Robots.txt Tester",
    page_icon="🤖",
    layout="wide",
)

TEXT_KEY = "robots_tester_text"
FETCH_KEY = "robots_tester_fetch"

PROFILES = {
    "Google": "google",
    "Bing": "bing",
    "Apple": "apple",
    "Стандарт robots.txt": "standard",
}

BOT_PRESETS = {
    "Googlebot": "Googlebot",
    "Googlebot Smartphone": "Googlebot",
    "Googlebot-Image": "Googlebot-Image",
    "Googlebot-Video": "Googlebot-Video",
    "Googlebot-News": "Googlebot-News",
    "Storebot-Google": "Storebot-Google",
    "Google-InspectionTool": "Google-InspectionTool",
    "GoogleOther": "GoogleOther",
    "GoogleOther-Image": "GoogleOther-Image",
    "GoogleOther-Video": "GoogleOther-Video",
    "AdsBot-Google": "AdsBot-Google",
    "AdsBot-Google-Mobile": "AdsBot-Google-Mobile",
    "Bingbot": "bingbot",
    "Bing Ads (adidxbot)": "adidxbot",
    "Applebot": "Applebot",
    "Applebot-Extended": "Applebot-Extended",
    "OAI-SearchBot": "OAI-SearchBot",
    "GPTBot": "GPTBot",
    "ChatGPT-User": "ChatGPT-User",
    "OAI-AdsBot": "OAI-AdsBot",
    "ClaudeBot": "ClaudeBot",
    "Claude-User": "Claude-User",
    "Claude-SearchBot": "Claude-SearchBot",
    "PerplexityBot": "PerplexityBot",
    "Perplexity-User": "Perplexity-User",
    "Власний user-agent": "",
}

KIND_LABELS = {
    "user-agent": "РОБОТ",
    "disallow": "ЗАБОРОНА",
    "allow": "ДОЗВІЛ",
    "sitemap": "SITEMAP",
    "crawl-delay": "ЗАТРИМКА",
    "comment": "КОМЕНТАР",
    "blank": "",
    "invalid": "ПОМИЛКА",
    "unknown": "НЕВІДОМО",
    "host": "СУМІСНІСТЬ",
    "clean-param": "СУМІСНІСТЬ",
}


def _render_line_breakdown(analysis, matched_line=None, visible_lines=None):
    rows = []
    lines = analysis.lines if visible_lines is None else visible_lines
    for line in lines:
        severity = ""
        if any(issue.severity == "error" for issue in line.issues):
            severity = "error"
        elif any(issue.severity == "risk" for issue in line.issues):
            severity = "risk"
        elif any(issue.severity == "warning" for issue in line.issues):
            severity = "warning"
        elif line.issues:
            severity = "info"
        classes = f"robots-row kind-{line.kind} severity-{severity}"
        if matched_line == line.number:
            classes += " matched"
        label = KIND_LABELS.get(line.kind, "ДИРЕКТИВА")
        badges = f'<span class="robots-badge">{html.escape(label)}</span>' if label else ""
        issue_text = " ".join(
            issue.message
            for issue in line.issues
            if issue.severity in {"error", "warning", "risk"}
        )
        details = line.explanation
        if issue_text:
            details += " " + issue_text
        raw = html.escape(line.raw) if line.raw else "&nbsp;"
        rows.append(
            f'<div class="{classes}">'
            f'<span class="robots-number">{line.number}</span>'
            f'<code class="robots-code">{raw}</code>'
            f'<span class="robots-meaning">{badges}{html.escape(details)}</span>'
            "</div>"
        )

    st.markdown(
        """
        <style>
        .robots-shell { border: 1px solid rgba(128,128,128,.28); border-radius: 12px; max-height: 560px; overflow-y: auto; }
        .robots-row { display: grid; grid-template-columns: 42px minmax(220px, .72fr) minmax(300px, 1.28fr); min-height: 38px; border-bottom: 1px solid rgba(128,128,128,.18); align-items: stretch; }
        .robots-row:last-child { border-bottom: 0; }
        .robots-number { padding: 8px 9px; text-align: right; color: #8a8f98; background: rgba(128,128,128,.08); font: 12px/1.5 ui-monospace,SFMono-Regular,Menlo,monospace; }
        .robots-code { padding: 8px 12px; white-space: pre-wrap; overflow-wrap: anywhere; color: inherit; background: rgba(128,128,128,.035); font-size: 13px; }
        .robots-meaning { padding: 7px 12px; font-size: 12px; line-height: 1.5; }
        .robots-badge { display: inline-block; min-width: 78px; margin-right: 9px; padding: 2px 7px; border-radius: 999px; text-align: center; font-size: 10px; font-weight: 700; letter-spacing: .035em; background: rgba(128,128,128,.14); }
        .kind-user-agent .robots-code, .kind-user-agent .robots-badge { color: #8b5cf6; }
        .kind-disallow .robots-code, .kind-disallow .robots-badge { color: #ef4444; }
        .kind-allow .robots-code, .kind-allow .robots-badge { color: #16a34a; }
        .kind-sitemap .robots-code, .kind-sitemap .robots-badge { color: #2684ff; }
        .kind-comment .robots-code, .kind-comment .robots-meaning { color: #8a8f98; }
        .kind-crawl-delay .robots-code, .kind-crawl-delay .robots-badge { color: #d97706; }
        .severity-error { box-shadow: inset 4px 0 #ef4444; background: rgba(239,68,68,.07); }
        .severity-risk { box-shadow: inset 4px 0 #f97316; background: rgba(249,115,22,.065); }
        .severity-warning { box-shadow: inset 4px 0 #f59e0b; background: rgba(245,158,11,.06); }
        .matched { outline: 2px solid #2684ff; outline-offset: -2px; background: rgba(38,132,255,.10); }
        @media (max-width: 900px) {
          .robots-row { grid-template-columns: 42px 1fr; }
          .robots-meaning { grid-column: 2; padding-top: 0; }
        }
        </style>
        <div class="robots-shell">"""
        + "".join(rows)
        + "</div>",
        unsafe_allow_html=True,
    )


if TEXT_KEY not in st.session_state:
    st.session_state[TEXT_KEY] = ""

st.markdown(
    """
    <style>
    .block-container {
        max-width: 1480px;
        padding-top: 1.25rem;
        padding-bottom: 2rem;
    }
    h1 { margin-bottom: .15rem; }
    div[data-testid="stTextArea"] textarea {
        font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace;
        font-size: 13px;
        line-height: 1.45;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

st.title("🤖 Robots.txt Tester")
st.caption(
    "Аналізує robots.txt, пояснює правила та перевіряє доступність URL для вибраного робота."
)

source_col, profile_col, button_col = st.columns([4.5, 1.45, 1.35], vertical_alignment="bottom")
with source_col:
    source_url = st.text_input(
        "Домен або URL robots.txt",
        placeholder="example.com або https://example.com/robots.txt",
    )
with profile_col:
    profile_label = st.selectbox(
        "Правила пошуковика",
        list(PROFILES),
        help="Деякі необов'язкові директиви підтримуються пошуковиками по-різному.",
    )
with button_col:
    load_clicked = st.button("⬇️ Завантажити", type="primary", use_container_width=True)

if load_clicked:
    st.session_state.pop(FETCH_KEY, None)
    try:
        with st.spinner("Завантажуємо robots.txt…"):
            fetched = fetch_robots(source_url)
        st.session_state[TEXT_KEY] = fetched.content
        st.session_state[FETCH_KEY] = fetched
        st.rerun()
    except RobotsFetchError as exc:
        if exc.status_code in {401, 403}:
            st.warning(
                f"Сайт заборонив автоматичне читання robots.txt "
                f"з нашого сервера (HTTP {exc.status_code}). Це не означає, "
                "що сайт закритий для пошукових роботів. Відкрийте файл "
                "у браузері, скопіюйте текст і вставте його в поле нижче."
            )
            if exc.url:
                st.link_button("🌐 Відкрити robots.txt у браузері", exc.url)
        else:
            st.error(str(exc))

fetched = st.session_state.get(FETCH_KEY)
if fetched:
    # Streamlit Cloud can keep a FetchResult created before a hot deploy in an
    # open browser session. New optional metadata must therefore have safe
    # defaults until that session downloads the file again.
    content_type = getattr(fetched, "content_type", "")
    encoding_errors = getattr(fetched, "encoding_errors", False)
    redirect_note = f" · редиректів: {len(fetched.redirects)}" if fetched.redirects else ""
    st.caption(
        f"Завантажено: HTTP {fetched.status_code} · {fetched.size:,} байт{redirect_note} · {fetched.final_url}".replace(",", " ")
    )
    if fetched.truncated:
        st.warning("Файл перевищує 500 КіБ. Аналізується лише перша частина файлу.")
    if "text/html" in content_type:
        st.warning(
            "Сервер повернув Content-Type: text/html замість звичайного тексту. "
            "Google спробує прочитати коректні правила, але сервер налаштований неправильно."
        )
    elif content_type and "text/plain" not in content_type:
        st.warning(
            f"Нетиповий Content-Type: {content_type}. Для robots.txt рекомендовано text/plain."
        )
    if encoding_errors:
        st.warning(
            "Файл містить байти поза UTF-8. Некоректні символи замінено, тому частина правил може не працювати."
        )

editor_col, tools_col = st.columns([1.35, 1], gap="large")
with editor_col:
    st.markdown("#### Вміст robots.txt")
    robots_text = st.text_area(
        "Вміст robots.txt",
        key=TEXT_KEY,
        height=410,
        label_visibility="collapsed",
        placeholder=(
            "User-agent: *\n"
            "Disallow: /admin/\n"
            "Allow: /admin/public/\n"
            "Sitemap: https://example.com/sitemap.xml"
        ),
    )
    st.download_button(
        "💾 Завантажити поточний файл",
        data=robots_text.encode("utf-8"),
        file_name="robots.txt",
        mime="text/plain; charset=utf-8",
        disabled=not robots_text.strip(),
        use_container_width=True,
    )

if not robots_text.strip():
    with tools_col:
        st.markdown("#### Перевірка")
        st.info(
            "Завантажте robots.txt із сайту або вставте його текст у редактор. "
            "Тут з'являться діагностика та перевірка конкретного URL."
        )
    st.stop()

analysis = analyze_robots(robots_text, PROFILES[profile_label])
errors = [issue for issue in analysis.issues if issue.severity == "error"]
warnings = [issue for issue in analysis.issues if issue.severity == "warning"]
risks = [issue for issue in analysis.issues if issue.severity == "risk"]
matched_line = None

with tools_col:
    st.markdown("#### Аналіз і перевірка")
    if risks:
        st.markdown(f"##### ⚠️ SEO-ризики: {len(risks)}")
        st.caption("Евристична перевірка: це не синтаксичні помилки, а сигнали для ручної перевірки.")
        for risk in risks[:3]:
            line_label = f"Рядок {risk.line}: " if risk.line else ""
            st.markdown(f"- **{line_label}**{risk.message.removeprefix('SEO-ризик: ')}")
        if len(risks) > 3:
            st.caption(f"Ще ризиків: {len(risks) - 3} — у построковому розборі нижче.")

    st.markdown("##### Перевірити конкретний URL")
    target_url = st.text_input(
        "URL або шлях",
        placeholder="https://example.com/admin/page або /admin/page",
    )
    bot_label = st.selectbox("Робот", list(BOT_PRESETS))
    user_agent = BOT_PRESETS[bot_label]
    if bot_label == "Власний user-agent":
        user_agent = st.text_input(
            "Власний user-agent",
            placeholder="Product token, наприклад ExampleBot",
        )

    robots_origin_url = fetched.requested_url if fetched else None
    if not robots_origin_url and source_url.strip():
        try:
            robots_origin_url = normalize_robots_url(source_url)
        except RobotsFetchError:
            pass

    result_signature = hash((robots_text, target_url, user_agent, profile_label, robots_origin_url))
    if st.button("🔎 Перевірити URL", type="primary", use_container_width=True):
        if not target_url.strip():
            st.error("Вкажіть URL або шлях для перевірки.")
        elif not user_agent.strip():
            st.error("Вкажіть user-agent для перевірки.")
        elif not re.fullmatch(r"[A-Za-z_-]+", user_agent.strip()):
            st.error("Вкажіть product token без версії та пробілів, наприклад Googlebot або ExampleBot.")
        else:
            try:
                result = test_url(analysis, target_url, user_agent, robots_origin_url)
            except ValueError as exc:
                st.error(str(exc))
            else:
                st.session_state["robots_tester_last_result"] = result
                st.session_state["robots_tester_last_signature"] = result_signature

    result = None
    if st.session_state.get("robots_tester_last_signature") == result_signature:
        result = st.session_state.get("robots_tester_last_result")

    if result:
        matched_line = result.matched_rule.line if result.matched_rule else None
        if result.allowed:
            st.success(f"✅ Дозволено для {bot_label}. {result.reason}")
        else:
            st.error(f"⛔ Заборонено для {bot_label}. {result.reason}")
        if result.matched_rule:
            st.code(
                f"{result.matched_rule.directive.title()}: {result.matched_rule.pattern}",
                language=None,
            )
        if result.applicable_group_indexes:
            group_agents = sorted(
                {
                    agent
                    for index in result.applicable_group_indexes
                    for agent in analysis.groups[index].agents
                }
            )
            st.caption("Застосована група: User-agent: " + ", ".join(group_agents))

problem_count = len(errors) + len(warnings) + len(risks)
st.markdown(
    """
    <style>
    div[data-testid="stExpander"] {
        border: 1px solid rgba(128,128,128,.28);
        background: rgba(128,128,128,.025);
    }
    div[data-testid="stExpander"] summary p {
        font-weight: 700;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

if problem_count:
    diagnostics = []
    if errors:
        diagnostics.append(f"🔴 Помилки: {len(errors)}")
    if risks:
        diagnostics.append(f"🟠 SEO-ризики: {len(risks)}")
    if warnings:
        diagnostics.append(f"🟡 Попередження: {len(warnings)}")
    breakdown_label = " · ".join(diagnostics) + " · Відкрити построковий розбір"
else:
    breakdown_label = "✅ Проблем не знайдено · Відкрити построковий розбір"

with st.expander(
    breakdown_label,
    expanded=False,
):
    st.caption(f"У файлі {len(analysis.lines)} рядків. Редактор прокручується окремо від сторінки.")
    leading_wildcards = [
        line
        for line in analysis.lines
        if line.name in {"allow", "disallow"} and line.value.startswith("*")
    ]
    if leading_wildcards and PROFILES[profile_label] in {"google", "bing"}:
        st.caption(
            f"{len(leading_wildcards)} правил починаються з «*». "
            f"{profile_label} підтримує такі шаблони; це не помилка."
        )

    only_problems = st.toggle(
        "Показати лише помилки, попередження й SEO-ризики",
        value=True,
    )
    if only_problems:
        visible_lines = [
            line
            for line in analysis.lines
            if any(issue.severity in {"error", "warning", "risk"} for issue in line.issues)
            or line.number == matched_line
        ]
        if visible_lines:
            _render_line_breakdown(analysis, matched_line, visible_lines)
        else:
            st.success("Помилок і попереджень немає.")
    else:
        visible_lines = [line for line in analysis.lines if line.kind != "blank"]
        st.caption(
            "Червоний — заборона, зелений — дозвіл, фіолетовий — група роботів. "
            "Синя рамка позначає правило, що спрацювало для URL."
        )
        _render_line_breakdown(analysis, matched_line, visible_lines)

    general_issues = [
        issue
        for issue in analysis.issues
        if issue.severity in {"error", "warning", "risk"} and issue.line is None
    ]
    for issue in general_issues:
        icon = {"error": "🔴", "risk": "🟠", "warning": "🟡"}[issue.severity]
        st.write(f"{icon} {issue.message}")
