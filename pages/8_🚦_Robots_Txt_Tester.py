import html

import streamlit as st

from robots_checker import RobotsFetchError, analyze_robots, fetch_robots, test_url


st.set_page_config(
    page_title="Robots.txt Tester",
    page_icon="🚦",
    layout="wide",
)

TEXT_KEY = "robots_tester_text"
FETCH_KEY = "robots_tester_fetch"

PROFILES = {
    "Google": "google",
    "Bing": "bing",
    "Стандарт robots.txt": "standard",
}

BOT_PRESETS = {
    "Googlebot": "Googlebot",
    "Googlebot Smartphone": "Mozilla/5.0 (Linux; Android 6.0.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Mobile Safari/537.36 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)",
    "Googlebot-Image": "Googlebot-Image",
    "Bingbot": "bingbot",
    "Bing Ads (adidxbot)": "adidxbot",
    "OAI-SearchBot": "OAI-SearchBot",
    "GPTBot": "GPTBot",
    "ChatGPT-User": "ChatGPT-User",
    "OAI-AdsBot": "OAI-AdsBot",
    "ClaudeBot": "ClaudeBot",
    "PerplexityBot": "PerplexityBot",
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


def _render_line_breakdown(analysis, matched_line=None):
    rows = []
    for line in analysis.lines:
        severity = ""
        if any(issue.severity == "error" for issue in line.issues):
            severity = "error"
        elif any(issue.severity == "warning" for issue in line.issues):
            severity = "warning"
        elif line.issues:
            severity = "info"
        classes = f"robots-row kind-{line.kind} severity-{severity}"
        if matched_line == line.number:
            classes += " matched"
        label = KIND_LABELS.get(line.kind, "ДИРЕКТИВА")
        badges = f'<span class="robots-badge">{html.escape(label)}</span>' if label else ""
        issue_text = " ".join(issue.message for issue in line.issues)
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
        .robots-shell { border: 1px solid rgba(128,128,128,.28); border-radius: 12px; overflow: hidden; }
        .robots-row { display: grid; grid-template-columns: 44px minmax(260px, .9fr) minmax(340px, 1.25fr); min-height: 43px; border-bottom: 1px solid rgba(128,128,128,.18); align-items: stretch; }
        .robots-row:last-child { border-bottom: 0; }
        .robots-number { padding: 11px 10px; text-align: right; color: #8a8f98; background: rgba(128,128,128,.08); font: 12px/1.6 ui-monospace,SFMono-Regular,Menlo,monospace; }
        .robots-code { padding: 11px 14px; white-space: pre-wrap; overflow-wrap: anywhere; color: inherit; background: rgba(128,128,128,.035); font-size: 13px; }
        .robots-meaning { padding: 10px 14px; font-size: 13px; line-height: 1.55; }
        .robots-badge { display: inline-block; min-width: 78px; margin-right: 9px; padding: 2px 7px; border-radius: 999px; text-align: center; font-size: 10px; font-weight: 700; letter-spacing: .035em; background: rgba(128,128,128,.14); }
        .kind-user-agent .robots-code, .kind-user-agent .robots-badge { color: #8b5cf6; }
        .kind-disallow .robots-code, .kind-disallow .robots-badge { color: #ef4444; }
        .kind-allow .robots-code, .kind-allow .robots-badge { color: #16a34a; }
        .kind-sitemap .robots-code, .kind-sitemap .robots-badge { color: #2684ff; }
        .kind-comment .robots-code, .kind-comment .robots-meaning { color: #8a8f98; }
        .kind-crawl-delay .robots-code, .kind-crawl-delay .robots-badge { color: #d97706; }
        .severity-error { box-shadow: inset 4px 0 #ef4444; background: rgba(239,68,68,.07); }
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

st.title("🚦 Robots.txt Tester")
st.caption(
    "Завантажте robots.txt із сайту або вставте його вручну. "
    "Сервіс пояснить кожен рядок і перевірить доступність конкретного URL."
)

with st.sidebar:
    st.markdown("## ⚙️ Налаштування")
    st.divider()
    profile_label = st.selectbox(
        "Правила пошуковика",
        list(PROFILES),
        help="Деякі необов'язкові директиви підтримуються пошуковиками по-різному.",
    )
    st.caption(
        "robots.txt керує скануванням, але сам по собі не гарантує "
        "видалення URL з індексу."
    )
    st.divider()
    st.caption("Robots.txt Tester v0.1")

st.markdown("### 1. Отримайте файл із сайту")
source_col, button_col = st.columns([5, 1])
with source_col:
    source_url = st.text_input(
        "Домен або URL robots.txt",
        placeholder="example.com або https://example.com/robots.txt",
        label_visibility="collapsed",
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
        st.error(str(exc))

fetched = st.session_state.get(FETCH_KEY)
if fetched:
    redirect_note = f" · редиректів: {len(fetched.redirects)}" if fetched.redirects else ""
    st.success(
        f"Завантажено: HTTP {fetched.status_code} · {fetched.size:,} байт{redirect_note} · {fetched.final_url}".replace(",", " ")
    )
    if fetched.truncated:
        st.warning("Файл перевищує 500 КіБ. Аналізується лише частина, яку враховує Google.")

st.markdown("### 2. Перевірте або відредагуйте правила")
robots_text = st.text_area(
    "Вміст robots.txt",
    key=TEXT_KEY,
    height=330,
    placeholder=(
        "User-agent: *\n"
        "Disallow: /admin/\n"
        "Allow: /admin/public/\n"
        "Sitemap: https://example.com/sitemap.xml"
    ),
)

st.download_button(
    "💾 Завантажити поточний robots.txt",
    data=robots_text.encode("utf-8"),
    file_name="robots.txt",
    mime="text/plain; charset=utf-8",
    disabled=not robots_text.strip(),
)

if not robots_text.strip():
    st.info("Завантажте robots.txt із сайту або вставте його текст у поле вище.")
    st.stop()

analysis = analyze_robots(robots_text, PROFILES[profile_label])
errors = [issue for issue in analysis.issues if issue.severity == "error"]
warnings = [issue for issue in analysis.issues if issue.severity == "warning"]

metric_cols = st.columns(5)
metric_cols[0].metric("Групи роботів", len(analysis.groups))
metric_cols[1].metric("Правила", len(analysis.rules))
metric_cols[2].metric("Sitemap", len(analysis.sitemaps))
metric_cols[3].metric("Помилки", len(errors))
metric_cols[4].metric("Попередження", len(warnings))

if errors:
    st.error(f"Знайдено помилок: {len(errors)}. Перевірте позначені рядки.")
elif warnings:
    st.warning(f"Критичних помилок немає, але є попередження: {len(warnings)}.")
else:
    st.success("Синтаксичних проблем не знайдено.")

st.markdown("### 3. Перевірте конкретний URL")
test_col, bot_col = st.columns([3, 2])
with test_col:
    target_url = st.text_input(
        "URL або шлях",
        placeholder="https://example.com/admin/page або /admin/page",
    )
with bot_col:
    bot_label = st.selectbox("Робот", list(BOT_PRESETS))

user_agent = BOT_PRESETS[bot_label]
if bot_label == "Власний user-agent":
    user_agent = st.text_input(
        "Власний user-agent",
        placeholder="Назва або повний user-agent робота",
    )

matched_line = None
if st.button("🔎 Перевірити URL", use_container_width=True):
    if not target_url.strip():
        st.error("Вкажіть URL або шлях для перевірки.")
    elif not user_agent.strip():
        st.error("Вкажіть user-agent для перевірки.")
    else:
        result = test_url(analysis, target_url, user_agent)
        matched_line = result.matched_rule.line if result.matched_rule else None
        st.session_state["robots_tester_last_match"] = matched_line
        st.session_state["robots_tester_last_signature"] = hash(robots_text)
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
elif st.session_state.get("robots_tester_last_signature") == hash(robots_text):
    matched_line = st.session_state.get("robots_tester_last_match")

st.markdown("### 4. Що означає кожен рядок")
st.caption(
    "Червоний — заборона, зелений — дозвіл, фіолетовий — група роботів. "
    "Рамкою позначається правило, що спрацювало під час останньої перевірки URL."
)
_render_line_breakdown(analysis, matched_line)

with st.expander("Усі зауваження"):
    if not analysis.issues:
        st.write("Зауважень немає.")
    for issue in analysis.issues:
        location = f"Рядок {issue.line}: " if issue.line else ""
        icon = {"error": "🔴", "warning": "🟡", "info": "🔵"}[issue.severity]
        st.write(f"{icon} {location}{issue.message}")
