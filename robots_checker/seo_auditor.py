from __future__ import annotations

import re
from typing import Any, Callable


SEO_ALLOW_RISK_PATTERNS = (
    (
        re.compile(
            r"(?:^|[/_*?&=.-])(?:checkout|check-out|cart|basket|shopping-bag|"
            r"order-confirmation|korzina|koshyk|koszyk|oformlenn(?:ya|ia|ie)(?:-zamovlennya|-zakaza)?)(?:$|[/_*?&=.-])"
            r"|(?:^|[/_*?&=.-])(?:кошик|корзина|оформлення(?:-замовлення)?)(?:$|[/_*?&=.-])"
        ),
        "правило дозволяє сканування кошика або оформлення замовлення. "
        "Такі URL зазвичай не мають пошукової цінності; перевірте, чи це навмисно.",
    ),
    (
        re.compile(
            r"(?:^|[/_*?&=.-])(?:login|log-in|signin|sign-in|signup|sign-up|register|"
            r"registration|account|profile|cabinet|kabinet|lichn(?:y|yi|ij)-kabinet|"
            r"customer-account|vkhid|voiti|reiestratsiia)(?:$|[/_*?&=.-])"
            r"|(?:^|[/_*?&=.-])(?:вхід|вход|реєстрація|регистрация|кабінет|профіль)(?:$|[/_*?&=.-])"
        ),
        "правило дозволяє сканування авторизації або особистого кабінету. "
        "Перевірте, чи ці службові сторінки мають потрапляти в обхід.",
    ),
    (
        re.compile(
            r"(?:^|[/_*?&=.-])(?:search|site-search|poshuk|poisk|filter|filters|facet|facets|"
            r"sort|orderby)(?:$|[/_*?&=.-])"
            r"|(?:^|[/_*?&=.-])(?:пошук|поиск|фільтр|фильтр|сортування|сортировка)(?:$|[/_*?&=.-])"
        ),
        "правило дозволяє сканування пошуку, фільтрів або сортування. "
        "Це може створювати багато дубльованих і малокорисних URL.",
    ),
    (
        re.compile(
            r"(?:^|[/_*?&=.-])(?:admin|administrator|wp-admin|api|graphql|ajax|internal|private)(?:$|[/_*?&=.-])"
            r"|(?:^|[/_*?&=.-])(?:адмін|админ|внутрішній)(?:$|[/_*?&=.-])"
        ),
        "правило дозволяє сканування технічного або адміністративного розділу. "
        "Перевірте, чи він справді має бути доступним роботам.",
    ),
)
SEO_IMPORTANT_SECTION_RE = re.compile(
    r"^/(?:[a-z]{2}(?:-[a-z]{2})?/)?(?:products?|catalog|category|categories|collections?|"
    r"shop|store|blog|articles?|news|katalog|tovary?|tovari|produkty?|kategori(?:ya|ia|i)|novyn[yi]|"
    r"каталог|товари|продукти|категорія|категории|магазин|блог|статті|статьи|новини|новости)/?$"
)
SEO_RENDER_RESOURCE_RE = re.compile(
    r"(?:^|[/_*?&=.-])(?:css|js|javascript|assets?|static|wp-content)(?:$|[/_*?&=.-])|"
    r"\.(?:css|js)(?:$|[?*$])"
)
SEO_IMAGE_RESOURCE_RE = re.compile(
    r"(?:^|[/_*?&=.-])(?:images?|img|media|uploads?)(?:$|[/_*?&=.-])|"
    r"\.(?:png|jpe?g|gif|webp|svg|avif)(?:$|[?*$])"
)
SEO_SAFE_ALLOW_RESOURCE_RE = re.compile(
    r"(?:admin-ajax|load-scripts|load-styles)\.php(?:$|[?*$])|"
    r"\.(?:css|js|mjs|png|jpe?g|gif|webp|svg|avif|woff2?|ttf)(?:$|[?*$])"
)
SEO_INDEXING_AGENTS = frozenset({"*", "googlebot", "bingbot", "applebot"})


def _seo_pattern(value: str) -> str:
    lowered = value.lower().split("#", 1)[0]
    lowered = re.sub(r"%2[fF]", "/", lowered)
    return re.sub(r"/+", "/", lowered)


def add_seo_risks(
    groups: list[Any],
    lines: list[Any],
    issues: list[Any],
    issue_factory: Callable[..., Any],
) -> None:
    existing = {(issue.line, issue.severity, issue.message) for issue in issues}

    def add(line_number: int, message: str) -> None:
        key = (line_number, "risk", message)
        if key in existing:
            return
        issue = issue_factory(line_number, "risk", message)
        lines[line_number - 1].issues.append(issue)
        issues.append(issue)
        existing.add(key)

    for group in groups:
        seen_rules: dict[str, Any] = {}
        closes_indexing_bot = bool(SEO_INDEXING_AGENTS.intersection(group.agents))

        for rule in group.rules:
            pattern = _seo_pattern(rule.pattern)

            if rule.directive == "allow" and not SEO_SAFE_ALLOW_RESOURCE_RE.search(pattern):
                for matcher, message in SEO_ALLOW_RISK_PATTERNS:
                    if matcher.search(pattern):
                        add(rule.line, f"SEO-ризик: {message}")
                        break

            if rule.directive == "disallow":
                broad_pattern = pattern.replace("*", "").removesuffix("$").rstrip("/") or "/"
                if pattern in {"/", "/*", "/*$"} and closes_indexing_bot:
                    add(
                        rule.line,
                        "SEO-ризик: це правило закриває весь сайт для основного пошукового робота. "
                        "Перевірте, чи сайт не закрито випадково.",
                    )
                elif SEO_IMPORTANT_SECTION_RE.fullmatch(broad_pattern):
                    add(
                        rule.line,
                        "SEO-ризик: можливо, закрито важливий контент — товари, категорії або статті. "
                        "Перевірте масштаб шаблону.",
                    )
                elif SEO_RENDER_RESOURCE_RE.search(pattern):
                    add(
                        rule.line,
                        "SEO-ризик: блокуються CSS, JavaScript або інші ресурси. "
                        "Пошуковик може неправильно відрендерити сторінку.",
                    )
                elif SEO_IMAGE_RESOURCE_RE.search(pattern):
                    add(
                        rule.line,
                        "SEO-ризик: блокуються зображення або медіафайли. "
                        "Це може завадити рендерингу та пошуку за зображеннями.",
                    )

            conflict = seen_rules.get(pattern)
            if conflict and conflict.directive != rule.directive:
                winner = "Allow" if "allow" in {conflict.directive, rule.directive} else rule.directive.title()
                add(
                    rule.line,
                    f"SEO-ризик: для шаблону «{rule.pattern}» є і Allow, і Disallow "
                    f"(рядки {conflict.line} та {rule.line}). За однакової специфічності перемагає {winner}; "
                    "перевірте задум.",
                )
            else:
                seen_rules.setdefault(pattern, rule)
