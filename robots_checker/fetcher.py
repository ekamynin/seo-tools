from __future__ import annotations

import ipaddress
import socket
from dataclasses import dataclass
from urllib.parse import urljoin, urlsplit, urlunsplit

import requests


MAX_ROBOTS_SIZE = 500 * 1024
MAX_REDIRECTS = 5


class RobotsFetchError(ValueError):
    def __init__(
        self,
        message: str,
        *,
        url: str | None = None,
        status_code: int | None = None,
    ) -> None:
        super().__init__(message)
        self.url = url
        self.status_code = status_code


@dataclass(frozen=True)
class FetchResult:
    content: str
    requested_url: str
    final_url: str
    status_code: int
    size: int
    truncated: bool
    redirects: tuple[str, ...]


def normalize_robots_url(value: str) -> str:
    candidate = value.strip()
    if not candidate:
        raise RobotsFetchError("Вкажіть домен або адресу robots.txt.")
    if "://" not in candidate:
        candidate = "https://" + candidate
    parts = urlsplit(candidate)
    if parts.scheme not in {"http", "https"} or not parts.hostname:
        raise RobotsFetchError("Підтримуються лише коректні адреси http:// та https://.")
    path = parts.path if parts.path.lower().endswith("/robots.txt") else "/robots.txt"
    return urlunsplit((parts.scheme, parts.netloc, path, parts.query if path == parts.path else "", ""))


def _validate_public_host(url: str) -> None:
    host = urlsplit(url).hostname
    if not host:
        raise RobotsFetchError("В адресі немає домену.")
    try:
        addresses = {item[4][0] for item in socket.getaddrinfo(host, None)}
    except socket.gaierror as exc:
        raise RobotsFetchError("Не вдалося знайти домен.") from exc
    if not addresses:
        raise RobotsFetchError("Домен не має доступної IP-адреси.")
    for address in addresses:
        ip = ipaddress.ip_address(address)
        if not ip.is_global:
            raise RobotsFetchError("З міркувань безпеки локальні та приватні адреси не перевіряються.")


def fetch_robots(value: str, timeout: float = 10.0) -> FetchResult:
    requested_url = normalize_robots_url(value)
    current_url = requested_url
    redirects: list[str] = []
    headers = {"User-Agent": "SEO-Tools-Robots-Tester/1.0"}

    for _ in range(MAX_REDIRECTS + 1):
        _validate_public_host(current_url)
        try:
            response = requests.get(
                current_url,
                headers=headers,
                timeout=timeout,
                allow_redirects=False,
                stream=True,
            )
        except requests.RequestException as exc:
            raise RobotsFetchError(f"Не вдалося завантажити robots.txt: {exc}") from exc

        if response.is_redirect or response.is_permanent_redirect:
            location = response.headers.get("Location")
            if not location:
                response.close()
                raise RobotsFetchError("Сервер повернув редирект без нової адреси.")
            current_url = urljoin(current_url, location)
            response.close()
            if urlsplit(current_url).scheme not in {"http", "https"}:
                raise RobotsFetchError("robots.txt перенаправляє на непідтримуваний протокол.")
            redirects.append(current_url)
            continue

        if response.status_code == 404:
            response.close()
            raise RobotsFetchError(
                "robots.txt не знайдено (HTTP 404). Для роботів це зазвичай "
                "означає відсутність обмежень.",
                url=current_url,
                status_code=404,
            )
        if not 200 <= response.status_code < 300:
            status_code = response.status_code
            response.close()
            raise RobotsFetchError(
                f"Сервер повернув HTTP {status_code} замість robots.txt.",
                url=current_url,
                status_code=status_code,
            )
        content_type = response.headers.get("Content-Type", "").lower()
        if "text/html" in content_type:
            response.close()
            raise RobotsFetchError(
                "Замість robots.txt сервер повернув HTML-сторінку. "
                "Перевірте адресу та налаштування сервера."
            )

        data = bytearray()
        truncated = False
        for chunk in response.iter_content(chunk_size=16 * 1024):
            if not chunk:
                continue
            remaining = MAX_ROBOTS_SIZE + 1 - len(data)
            data.extend(chunk[:remaining])
            if len(data) > MAX_ROBOTS_SIZE:
                truncated = True
                del data[MAX_ROBOTS_SIZE:]
                break
        content = bytes(data).decode("utf-8-sig", errors="replace")
        response.close()
        return FetchResult(
            content=content,
            requested_url=requested_url,
            final_url=current_url,
            status_code=response.status_code,
            size=len(data),
            truncated=truncated,
            redirects=tuple(redirects),
        )

    raise RobotsFetchError(f"Забагато редиректів — більше {MAX_REDIRECTS}.")
