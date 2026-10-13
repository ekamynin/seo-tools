from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Literal
from urllib.parse import urlsplit


Severity = Literal["error", "warning", "info"]


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


def analyze_robots(text: str, profile: str = "google") -> Analysis:
    lines: list[ParsedLine] = []
    groups: list[Group] = []
    issues: list[Issue] = []
    sitemaps: list[str] = []
    current_group: Group | None = None

    for number, raw in enumerate(text.splitlines(), start=1):
        stripped = raw.strip()
        if not stripped:
            lines.append(ParsedLine(number, raw, "blank", explanation="Порожній рядок."))
            continue
        if stripped.startswith("#"):
            lines.append(ParsedLine(number, raw, "comment", explanation="Коментар; роботи його ігнорують."))
            continue

        content = raw.split("#", 1)[0].strip()
        if ":" not in content:
            issue = Issue(number, "error", "Немає двокрапки між директивою та значенням.")
            parsed = ParsedLine(number, raw, "invalid", explanation="Рядок має неправильний формат.", issues=[issue])
            lines.append(parsed)
            issues.append(issue)
            continue

        raw_name, raw_value = content.split(":", 1)
        name = raw_name.strip().lower()
        value = raw_value.strip()
        kind = name if name in SUPPORTED_DIRECTIVES | KNOWN_BUT_ENGINE_SPECIFIC else "unknown"
        parsed = ParsedLine(number, raw, kind, name, value, _explanation(name, value))

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
            current_group.agents.append(value.lower())
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
                severity = "warning" if profile == "standard" else "info"
                issue = Issue(
                    number,
                    severity,
                    "Початковий символ «*» підтримується Google і Bing. "
                    "Для суворої відповідності RFC можна почати шаблон з «/*»."
                )
                parsed.issues.append(issue)
            elif value and not value.startswith("/"):
                issue = Issue(
                    number,
                    "error",
                    "Шлях у Allow або Disallow має починатися з «/» або «*».",
                )
                parsed.issues.append(issue)
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
            sitemaps.append(value)
            parts = urlsplit(value)
            if not value or parts.scheme not in {"http", "https"} or not parts.netloc:
                issue = Issue(number, "warning", "Sitemap має містити повну адресу з http:// або https://.")
                parsed.issues.append(issue)
        elif name == "crawl-delay":
            if current_group is not None:
                current_group.has_supported_directive = True
                current_group.line_numbers.append(number)
                parsed.group_index = current_group.index
            if profile == "google":
                issue = Issue(number, "warning", "Google не підтримує Crawl-delay у robots.txt.")
                parsed.issues.append(issue)
            elif not re.fullmatch(r"\d+(?:\.\d+)?", value):
                issue = Issue(number, "warning", "Crawl-delay зазвичай має бути додатним числом.")
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

    return Analysis(lines=lines, groups=groups, issues=issues, sitemaps=sitemaps)


def _agent_score(group: Group, user_agent: str) -> int | None:
    lowered = user_agent.lower()
    scores: list[int] = []
    for agent in group.agents:
        if agent == "*":
            scores.append(0)
        elif agent and agent in lowered:
            scores.append(len(agent))
    return max(scores) if scores else None


def _rule_regex(pattern: str) -> re.Pattern[str]:
    anchored = pattern.endswith("$")
    body = pattern[:-1] if anchored else pattern
    expression = re.escape(body).replace(r"\*", ".*")
    if anchored:
        expression += "$"
    return re.compile("^" + expression)


def _matching_length(pattern: str) -> int:
    return len(pattern.rstrip("$").replace("*", ""))


def test_url(analysis: Analysis, target: str, user_agent: str) -> TestResult:
    if not user_agent.strip():
        raise ValueError("Вкажіть user-agent для перевірки.")
    parts = urlsplit(target if "://" in target else "https://example.invalid" + (target if target.startswith("/") else "/" + target))
    path = parts.path or "/"
    if parts.query:
        path += "?" + parts.query

    scored = [(group, _agent_score(group, user_agent)) for group in analysis.groups]
    matches = [(group, score) for group, score in scored if score is not None]
    if not matches:
        return TestResult(True, user_agent, target, path, None, (), "Немає групи для цього робота; сканування дозволене за замовчуванням.")

    best_score = max(score for _, score in matches)
    applicable = [group for group, score in matches if score == best_score]
    matching_rules = [
        rule
        for group in applicable
        for rule in group.rules
        if _rule_regex(rule.pattern).search(path)
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
        key=lambda rule: (_matching_length(rule.pattern), rule.directive == "allow"),
        reverse=True,
    )
    winner = matching_rules[0]
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
