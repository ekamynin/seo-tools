from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Literal
from urllib.parse import urlsplit


Severity = Literal["error", "warning", "risk", "info"]


@dataclass(frozen=True)
class Issue:
    line: int | None
    severity: Severity
    message: str


@dataclass
class ParsedLine:
    number: int
    raw: str
    kind: str
    name: str = ""
    value: str = ""
    explanation: str = ""
    group_index: int | None = None
    issues: list[Issue] = field(default_factory=list)


@dataclass(frozen=True)
class Rule:
    directive: Literal["allow", "disallow"]
    pattern: str
    line: int
    group_index: int


@dataclass
class Group:
    index: int
    agents: list[str] = field(default_factory=list)
    rules: list[Rule] = field(default_factory=list)
    line_numbers: list[int] = field(default_factory=list)
    has_supported_directive: bool = False


@dataclass
class Analysis:
    lines: list[ParsedLine]
    groups: list[Group]
    issues: list[Issue]
    sitemaps: list[str]
    profile: str = "google"

    @property
    def rules(self) -> list[Rule]:
        return [rule for group in self.groups for rule in group.rules]


@dataclass(frozen=True)
class TestResult:
    allowed: bool
    user_agent: str
    target: str
    path: str
    matched_rule: Rule | None
    applicable_group_indexes: tuple[int, ...]
    reason: str


SUPPORTED_DIRECTIVES = {"user-agent", "allow", "disallow", "sitemap"}
KNOWN_BUT_ENGINE_SPECIFIC = {"crawl-delay", "host", "clean-param"}
GOOGLE_DIRECTIVE_ALIASES = {
    "useragent": "user-agent",
    "user agent": "user-agent",
    "dissallow": "disallow",
    "dissalow": "disallow",
    "disalow": "disallow",
    "diasllow": "disallow",
    "disallaw": "disallow",
    "site-map": "sitemap",
}
GOOGLE_AGENT_FALLBACKS = {
    "googlebot-image": "googlebot",
    "googlebot-video": "googlebot",
    "googlebot-news": "googlebot",
    "google-inspectiontool": "googlebot",
    "google-cloudvertexbot": "googlebot",
    "googleother-image": "googleother",
    "googleother-video": "googleother",
}
PRODUCT_TOKEN_RE = re.compile(r"[A-Za-z_-]+")
PERCENT_ESCAPE_RE = re.compile(r"%[0-9A-Fa-f]{2}")
UNRESERVED = frozenset("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-._~")

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


def _explanation(name: str, value: str) -> str:
    if name == "user-agent":
        if value == "*":
            return "Початок групи правил для всіх роботів, якщо немає конкретнішої групи."
        return f"Початок або продовження групи правил для робота «{value}»."
    if name == "disallow":
        if not value:
            return "Порожня заборона нічого не блокує."
        if value == "/":
            return "Забороняє цьому роботу сканувати весь сайт."
        return f"Забороняє сканування URL, шлях яких відповідає шаблону «{value}»."
    if name == "allow":
        if not value:
            return "Порожній дозвіл ігнорується."
        if value == "/":
            return "Дозволяє сканування всього сайту в межах цієї групи."
        return f"Дозволяє сканування URL, шлях яких відповідає шаблону «{value}»."
    if name == "sitemap":
        return f"Повідомляє пошуковикам адресу XML-карти: {value or 'адресу не вказано'}."
    if name == "crawl-delay":
        return "Задає затримку між запитами для роботів, які підтримують цю директиву."
    if name == "noindex":
        return "Ця директива не є підтримуваним способом заборонити індексацію."
    if name in {"host", "clean-param"}:
        return f"Директива «{name}» підтримується не всіма пошуковими системами."
    return f"Невідома директива «{name}» буде проігнорована більшістю роботів."


def _add_line_issue(
    lines: list[ParsedLine],
    issues: list[Issue],
    line_number: int,
    severity: Severity,
    message: str,
) -> None:
    issue = Issue(line_number, severity, message)
    lines[line_number - 1].issues.append(issue)
    issues.append(issue)


def _seo_pattern(value: str) -> str:
    lowered = value.lower().split("#", 1)[0]
    lowered = re.sub(r"%2[fF]", "/", lowered)
    return re.sub(r"/+", "/", lowered)


def _add_seo_risks(
    groups: list[Group],
    lines: list[ParsedLine],
    issues: list[Issue],
) -> None:
    for group in groups:
        seen_rules: dict[str, Rule] = {}
        closes_indexing_bot = bool(SEO_INDEXING_AGENTS.intersection(group.agents))

        for rule in group.rules:
            pattern = _seo_pattern(rule.pattern)

            if rule.directive == "allow" and not SEO_SAFE_ALLOW_RESOURCE_RE.search(pattern):
                for matcher, message in SEO_ALLOW_RISK_PATTERNS:
                    if matcher.search(pattern):
                        _add_line_issue(
                            lines,
                            issues,
                            rule.line,
                            "risk",
                            f"SEO-ризик: {message}",
                        )
                        break

            if rule.directive == "disallow":
                broad_pattern = pattern.replace("*", "").removesuffix("$").rstrip("/") or "/"
                if pattern in {"/", "/*", "/*$"} and closes_indexing_bot:
                    _add_line_issue(
                        lines,
                        issues,
                        rule.line,
                        "risk",
                        "SEO-ризик: це правило закриває весь сайт для основного пошукового робота. "
                        "Перевірте, чи сайт не закрито випадково.",
                    )
                elif SEO_IMPORTANT_SECTION_RE.fullmatch(broad_pattern):
                    _add_line_issue(
                        lines,
                        issues,
                        rule.line,
                        "risk",
                        "SEO-ризик: можливо, закрито важливий контент — товари, категорії або статті. "
                        "Перевірте масштаб шаблону.",
                    )
                elif SEO_RENDER_RESOURCE_RE.search(pattern):
                    _add_line_issue(
                        lines,
                        issues,
                        rule.line,
                        "risk",
                        "SEO-ризик: блокуються CSS, JavaScript або інші ресурси. "
                        "Пошуковик може неправильно відрендерити сторінку.",
                    )
                elif SEO_IMAGE_RESOURCE_RE.search(pattern):
                    _add_line_issue(
                        lines,
                        issues,
                        rule.line,
                        "risk",
                        "SEO-ризик: блокуються зображення або медіафайли. "
                        "Це може завадити рендерингу та пошуку за зображеннями.",
                    )

            conflict = seen_rules.get(pattern)
            if conflict and conflict.directive != rule.directive:
                winner = "Allow" if "allow" in {conflict.directive, rule.directive} else rule.directive.title()
                _add_line_issue(
                    lines,
                    issues,
                    rule.line,
                    "risk",
                    f"SEO-ризик: для шаблону «{rule.pattern}» є і Allow, і Disallow "
                    f"(рядки {conflict.line} та {rule.line}). За однакової специфічності перемагає {winner}; "
                    "перевірте задум.",
                )
            else:
                seen_rules.setdefault(pattern, rule)


def analyze_robots(text: str, profile: str = "google") -> Analysis:
    lines: list[ParsedLine] = []
    groups: list[Group] = []
    issues: list[Issue] = []
    sitemaps: list[str] = []
    current_group: Group | None = None

    encoded_text = text.encode("utf-8")
    if profile == "google" and len(encoded_text) > 500 * 1024:
        text = encoded_text[: 500 * 1024].decode("utf-8", errors="ignore")
        issues.append(
            Issue(
                None,
                "warning",
                "Google читає лише перші 500 КіБ robots.txt; решту файлу не проаналізовано.",
            )
        )

    for number, raw in enumerate(text.splitlines(), start=1):
        parse_raw = raw[1:] if number == 1 and raw.startswith("\ufeff") else raw
        stripped = parse_raw.strip()
        if not stripped:
            lines.append(ParsedLine(number, raw, "blank", explanation="Порожній рядок."))
            continue
        if stripped.startswith("#"):
            lines.append(ParsedLine(number, raw, "comment", explanation="Коментар; роботи його ігнорують."))
            continue

        content = parse_raw.split("#", 1)[0].strip()
        missing_colon = False
        if ":" in content:
            raw_name, raw_value = content.split(":", 1)
        else:
            parts = content.split()
            candidate_name = parts[0].lower() if parts else ""
            canonical_candidate = GOOGLE_DIRECTIVE_ALIASES.get(candidate_name, candidate_name)
            if profile == "google" and len(parts) == 2 and canonical_candidate in {
                "user-agent",
                "allow",
                "disallow",
                "sitemap",
            }:
                raw_name, raw_value = parts
                missing_colon = True
            else:
                issue = Issue(number, "error", "Немає двокрапки між директивою та значенням.")
                parsed = ParsedLine(number, raw, "invalid", explanation="Рядок має неправильний формат.", issues=[issue])
                lines.append(parsed)
                issues.append(issue)
                continue

        original_name = raw_name.strip().lower()
        name = GOOGLE_DIRECTIVE_ALIASES.get(original_name, original_name) if profile == "google" else original_name
        value = raw_value.strip()
        kind = name if name in SUPPORTED_DIRECTIVES | KNOWN_BUT_ENGINE_SPECIFIC else "unknown"
        parsed = ParsedLine(number, raw, kind, name, value, _explanation(name, value))

        if missing_colon:
            parsed.issues.append(
                Issue(number, "warning", "Google зрозуміє цей рядок без двокрапки, але краще додати «:».")
            )
        if name != original_name:
            parsed.issues.append(
                Issue(
                    number,
                    "warning",
                    f"Google розпізнає «{original_name}» як «{name}», але виправте назву директиви.",
                )
            )

        if not name:
            issue = Issue(number, "error", "Перед двокрапкою не вказано назву директиви.")
            parsed.kind = "invalid"
            parsed.issues.append(issue)
        elif name == "user-agent":
            if not value:
                issue = Issue(number, "error", "User-agent не може бути порожнім.")
                parsed.issues.append(issue)
            if current_group is None or current_group.has_supported_directive:
                current_group = Group(index=len(groups))
                groups.append(current_group)
            normalized_agent = ""
            if value.startswith("*"):
                normalized_agent = "*"
                if value != "*":
                    severity = "warning" if profile != "google" else "info"
                    parsed.issues.append(
                        Issue(number, severity, "Після «*» у User-agent є зайвий текст; він ігнорується.")
                    )
            elif value:
                token_match = PRODUCT_TOKEN_RE.match(value)
                normalized_agent = token_match.group(0).lower() if token_match else ""
                if not normalized_agent:
                    parsed.issues.append(
                        Issue(number, "error", "User-agent має містити product token із літер, «-» або «_».")
                    )
                elif normalized_agent != value.lower():
                    if profile == "google":
                        parsed.issues.append(
                            Issue(
                                number,
                                "warning",
                                f"Google врахує лише product token «{normalized_agent}»; решта значення ігнорується.",
                            )
                        )
                    else:
                        parsed.issues.append(
                            Issue(
                                number,
                                "error",
                                "User-agent має бути одним product token без версії, пробілів або «*» у кінці.",
                            )
                        )
                        normalized_agent = ""
            if normalized_agent:
                current_group.agents.append(normalized_agent)
            current_group.line_numbers.append(number)
            parsed.group_index = current_group.index
        elif name in {"allow", "disallow"}:
            if current_group is None:
                issue = Issue(number, "error", f"{name.title()} розміщено до першого User-agent і буде проігноровано.")
                parsed.issues.append(issue)
            else:
                current_group.has_supported_directive = True
                current_group.line_numbers.append(number)
                parsed.group_index = current_group.index
                if value:
                    current_group.rules.append(Rule(name, value, number, current_group.index))
            if value.startswith("*"):
                issue = Issue(
                    number,
                    "info",
                    "Початковий символ «*» підтримується RFC, Google і Bing та може охопити початковий «/».",
                )
                parsed.issues.append(issue)
            elif value and not value.startswith("/"):
                issue = Issue(
                    number,
                    "error",
                    "Шлях у Allow або Disallow має починатися з «/» або «*».",
                )
                parsed.issues.append(issue)
            if value and re.search(r"%(?![0-9A-Fa-f]{2})", value):
                parsed.issues.append(
                    Issue(number, "warning", "Некоректне percent-кодування: після «%» мають бути дві hex-цифри.")
                )
            if any((ord(char) < 32 and char != "\t") or ord(char) == 127 for char in value):
                parsed.issues.append(
                    Issue(number, "error", "Шлях містить керівний символ, який не дозволений у robots.txt.")
                )
            if not value:
                if name == "allow":
                    issue = Issue(
                        number,
                        "warning",
                        "Порожній Allow нічого не відкриває. Вкажіть шлях, наприклад Allow: /.",
                    )
                else:
                    issue = Issue(
                        number,
                        "info",
                        "Порожній Disallow нічого не блокує; сканування залишається дозволеним.",
                    )
                parsed.issues.append(issue)
        elif name == "sitemap":
            if value:
                sitemaps.append(value)
            parts = urlsplit(value)
            if not value or parts.scheme not in {"http", "https"} or not parts.netloc:
                issue = Issue(number, "warning", "Sitemap має містити повну адресу з http:// або https://.")
                parsed.issues.append(issue)
        elif name == "crawl-delay":
            if current_group is not None:
                if profile == "bing":
                    current_group.has_supported_directive = True
                current_group.line_numbers.append(number)
                parsed.group_index = current_group.index
            if profile == "google":
                issue = Issue(number, "warning", "Google не підтримує Crawl-delay у robots.txt.")
                parsed.issues.append(issue)
            elif profile == "apple":
                issue = Issue(number, "warning", "Applebot не підтримує Crawl-delay у robots.txt.")
                parsed.issues.append(issue)
            elif profile == "bing" and not re.fullmatch(r"(?:[1-9]|1\d|20)", value):
                issue = Issue(number, "warning", "Bing приймає Crawl-delay як ціле число від 1 до 20 секунд.")
                parsed.issues.append(issue)
            elif profile == "standard":
                issue = Issue(number, "warning", "Crawl-delay не входить до RFC 9309; підтримка залежить від робота.")
                parsed.issues.append(issue)
        elif name == "noindex":
            issue = Issue(number, "error", "Noindex у robots.txt не підтримується. Використайте meta robots або X-Robots-Tag.")
            parsed.issues.append(issue)
        elif name in {"host", "clean-param"}:
            issue = Issue(number, "warning", f"{name.title()} не входить до стандарту та ігнорується Google і Bing.")
            parsed.issues.append(issue)
        else:
            issue = Issue(number, "warning", f"Невідома директива «{name}» буде проігнорована.")
            parsed.issues.append(issue)

        lines.append(parsed)
        issues.extend(parsed.issues)

    if not groups:
        issue = Issue(None, "warning", "У файлі немає жодної групи User-agent.")
        issues.append(issue)
    for group in groups:
        if not any(group.agents):
            continue
        if not group.rules:
            line = group.line_numbers[0] if group.line_numbers else None
            issue = Issue(line, "info", "У цієї групи немає Allow або Disallow; сканування дозволене за замовчуванням.")
            issues.append(issue)
            if line is not None:
                lines[line - 1].issues.append(issue)

    _add_seo_risks(groups, lines, issues)

    return Analysis(lines=lines, groups=groups, issues=issues, sitemaps=sitemaps, profile=profile)


def _agent_score(group: Group, user_agent: str, profile: str) -> int | None:
    lowered = user_agent.strip().lower()
    valid_product_token = bool(PRODUCT_TOKEN_RE.fullmatch(lowered))
    scores: list[int] = []
    for agent in group.agents:
        if agent == "*":
            if not (profile == "google" and "adsbot-google" in lowered):
                scores.append(0)
            continue

        is_direct_match = valid_product_token and lowered == agent
        if is_direct_match:
            scores.append(1000 + len(agent))
            continue

        if profile == "google" and GOOGLE_AGENT_FALLBACKS.get(lowered) == agent:
            scores.append(500 + len(agent))
        elif profile == "bing" and agent == "msnbot" and lowered == "bingbot":
            scores.append(500 + len(agent))
        elif profile == "apple" and agent == "googlebot" and lowered == "applebot":
            scores.append(500 + len(agent))
    return max(scores) if scores else None


def _canonicalize_for_match(value: str, profile: str, *, target: bool = False) -> str:
    result: list[str] = []
    index = 0
    while index < len(value):
        char = value[index]
        if char == "%" and PERCENT_ESCAPE_RE.match(value, index):
            escaped = value[index + 1 : index + 3].upper()
            decoded = chr(int(escaped, 16))
            if profile in {"standard", "apple"} and decoded in UNRESERVED:
                result.append(decoded)
            else:
                result.append("%" + escaped)
            index += 3
            continue
        if ord(char) > 127:
            result.extend(f"%{byte:02X}" for byte in char.encode("utf-8"))
        elif target and char in {"*", "$"}:
            result.append(f"%{ord(char):02X}")
        else:
            result.append(char)
        index += 1
    return "".join(result)


def _glob_matches(path: str, pattern: str) -> bool:
    anchored = pattern.endswith("$")
    body = pattern[:-1] if anchored else pattern
    if not anchored:
        body += "*"

    path_index = 0
    pattern_index = 0
    star_index = -1
    retry_path_index = -1
    while path_index < len(path):
        if pattern_index < len(body) and body[pattern_index] == "*":
            star_index = pattern_index
            retry_path_index = path_index
            pattern_index += 1
        elif pattern_index < len(body) and body[pattern_index] == path[path_index]:
            pattern_index += 1
            path_index += 1
        elif star_index != -1:
            pattern_index = star_index + 1
            retry_path_index += 1
            path_index = retry_path_index
        else:
            return False
    while pattern_index < len(body) and body[pattern_index] == "*":
        pattern_index += 1
    return pattern_index == len(body)


def _matching_length(pattern: str, profile: str) -> int:
    canonical = _canonicalize_for_match(pattern, profile)
    if profile not in {"standard", "apple"}:
        return len(canonical)
    length = 0
    index = 0
    while index < len(canonical):
        if canonical[index] == "%" and PERCENT_ESCAPE_RE.match(canonical, index):
            index += 3
        else:
            index += 1
        length += 1
    return length


def _rule_priority(rule: Rule, path: str, profile: str) -> int | None:
    pattern = _canonicalize_for_match(rule.pattern, profile)
    if _glob_matches(path, pattern):
        return _matching_length(rule.pattern, profile)

    if profile == "google" and rule.directive == "allow":
        slash_position = rule.pattern.rfind("/")
        if slash_position >= 0 and rule.pattern[slash_position:].startswith("/index.htm"):
            directory_pattern = rule.pattern[: slash_position + 1] + "$"
            canonical_directory = _canonicalize_for_match(directory_pattern, profile)
            if _glob_matches(path, canonical_directory):
                return _matching_length(directory_pattern, profile)
    return None


def _origin(url: str) -> tuple[str, str, int | None] | None:
    parts = urlsplit(url)
    if parts.scheme not in {"http", "https"} or not parts.hostname:
        return None
    try:
        host = parts.hostname.encode("idna").decode("ascii").lower()
        port = parts.port
    except (UnicodeError, ValueError):
        return None
    default_port = 80 if parts.scheme == "http" else 443
    return parts.scheme.lower(), host, port or default_port


def test_url(
    analysis: Analysis,
    target: str,
    user_agent: str,
    robots_url: str | None = None,
) -> TestResult:
    if not user_agent.strip():
        raise ValueError("Вкажіть user-agent для перевірки.")
    has_absolute_target = "://" in target
    parts = urlsplit(target if has_absolute_target else "https://example.invalid" + (target if target.startswith("/") else "/" + target))
    path = parts.path or "/"
    if parts.query:
        path += "?" + parts.query

    target_origin = _origin(target) if has_absolute_target else None
    if has_absolute_target and target_origin is None:
        raise ValueError("Вкажіть коректний URL з протоколом http:// або https://.")
    robots_origin = _origin(robots_url) if robots_url else None
    if target_origin and robots_origin and target_origin != robots_origin:
        return TestResult(
            True,
            user_agent,
            target,
            path,
            None,
            (),
            "Цей robots.txt не застосовується до URL з іншим протоколом, доменом або портом.",
        )
    canonical_path = _canonicalize_for_match(path, analysis.profile, target=True)

    if canonical_path == "/robots.txt":
        return TestResult(
            True,
            user_agent,
            target,
            path,
            None,
            (),
            "Файл /robots.txt завжди доступний роботу для читання правил.",
        )

    scored = [
        (group, _agent_score(group, user_agent, analysis.profile))
        for group in analysis.groups
    ]
    matches = [(group, score) for group, score in scored if score is not None]
    if not matches:
        return TestResult(True, user_agent, target, path, None, (), "Немає групи для цього робота; сканування дозволене за замовчуванням.")

    best_score = max(score for _, score in matches)
    applicable = [group for group, score in matches if score == best_score]
    matching_rules = [
        (rule, priority)
        for group in applicable
        for rule in group.rules
        if (priority := _rule_priority(rule, canonical_path, analysis.profile)) is not None
    ]
    if not matching_rules:
        return TestResult(
            True,
            user_agent,
            target,
            path,
            None,
            tuple(group.index for group in applicable),
            "У вибраній групі немає правила, що відповідає цьому URL; сканування дозволене.",
        )

    matching_rules.sort(
        key=lambda item: (item[1], item[0].directive == "allow"),
        reverse=True,
    )
    winner = matching_rules[0][0]
    allowed = winner.directive == "allow"
    action = "дозволяє" if allowed else "забороняє"
    return TestResult(
        allowed,
        user_agent,
        target,
        path,
        winner,
        tuple(group.index for group in applicable),
        f"Рядок {winner.line} є найконкретнішим відповідним правилом і {action} сканування.",
    )
