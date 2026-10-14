from __future__ import annotations

import ipaddress
import socket
from dataclasses import dataclass
from urllib.parse import urljoin, urlsplit, urlunsplit

import requests


MAX_ROBOTS_SIZE = 500 * 1024
MAX_REDIRECTS = 5
MAX_URL_LENGTH = 2048
ALLOWED_PORTS = {80, 443}


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
    content_type: str
    encoding_errors: bool


def _split_public_http_url(value: str):
    if len(value) > MAX_URL_LENGTH:
        raise RobotsFetchError(f"Адреса занадто довга — максимум {MAX_URL_LENGTH} символів.")
    if any(ord(char) < 32 or ord(char) == 127 for char in value):
        raise RobotsFetchError("Адреса містить недопустимий керівний символ.")
    try:
        parts = urlsplit(value)
        hostname = parts.hostname
        port = parts.port
    except (UnicodeError, ValueError) as exc:
        raise RobotsFetchError("Вкажіть коректну адресу сайту.") from exc
    if parts.scheme.lower() not in {"http", "https"} or not hostname:
        raise RobotsFetchError("Підтримуються лише коректні адреси http:// та https://.")
    if parts.username or parts.password:
        raise RobotsFetchError("Адреси з логіном або паролем не підтримуються.")
    if port is not None and port not in ALLOWED_PORTS:
        raise RobotsFetchError("З міркувань безпеки дозволені лише стандартні порти 80 і 443.")
    try:
        hostname.encode("idna")
    except UnicodeError as exc:
        raise RobotsFetchError("Адреса містить некоректне ім'я домену.") from exc
    return parts


def normalize_robots_url(value: str) -> str:
    candidate = value.strip()
    if not candidate:
        raise RobotsFetchError("Вкажіть домен або адресу robots.txt.")
    if "://" not in candidate:
        candidate = "https://" + candidate
    parts = _split_public_http_url(candidate)
    return urlunsplit((parts.scheme.lower(), parts.netloc, "/robots.txt", "", ""))


def _validate_public_host(url: str) -> None:
    parts = _split_public_http_url(url)
    host = parts.hostname
    if not host:
        raise RobotsFetchError("В адресі немає домену.")
    try:
        addresses = {item[4][0] for item in socket.getaddrinfo(host, None)}
    except (socket.gaierror, OSError, UnicodeError) as exc:
        raise RobotsFetchError("Не вдалося знайти домен.") from exc
    if not addresses:
        raise RobotsFetchError("Домен не має доступної IP-адреси.")
    for address in addresses:
        try:
            ip = ipaddress.ip_address(address)
        except ValueError as exc:
            raise RobotsFetchError("Домен повернув некоректну IP-адресу.") from exc
        if not ip.is_global:
            raise RobotsFetchError("З міркувань безпеки локальні та приватні адреси не перевіряються.")


def _peer_ip(response: requests.Response) -> str | None:
    """Best-effort extraction of the actual socket peer used by urllib3."""
    candidates = [
        getattr(getattr(response.raw, "_connection", None), "sock", None),
        getattr(getattr(response.raw, "connection", None), "sock", None),
    ]
    try:
        candidates.append(response.raw._fp.fp.raw._sock)
    except AttributeError:
        pass
    for sock in candidates:
        if sock is None:
            continue
        try:
            return sock.getpeername()[0]
        except (AttributeError, OSError, TypeError):
            continue
    return None


def _validate_connected_peer(response: requests.Response) -> None:
    # Mock responses in unit tests do not have a real socket. Real requests do;
    # failing closed here also protects against DNS rebinding between lookup and connect.
    if not isinstance(response, requests.Response):
        return
    address = _peer_ip(response)
    if not address:
        raise RobotsFetchError("Не вдалося безпечно перевірити адресу сервера.")
    try:
        ip = ipaddress.ip_address(address)
    except ValueError as exc:
        raise RobotsFetchError("Сервер повернув некоректну IP-адресу.") from exc
    if not ip.is_global:
        raise RobotsFetchError("З міркувань безпеки локальні та приватні адреси не перевіряються.")


def fetch_robots(value: str, timeout: float = 10.0) -> FetchResult:
    requested_url = normalize_robots_url(value)
    current_url = requested_url
    redirects: list[str] = []
    headers = {"User-Agent": "SEO-Tools-Robots-Tester/1.0"}

    session = requests.Session()
    session.trust_env = False
    try:
        for _ in range(MAX_REDIRECTS + 1):
            _validate_public_host(current_url)
            try:
                response = session.get(
                    current_url,
                    headers=headers,
                    timeout=timeout,
                    allow_redirects=False,
                    stream=True,
                )
            except requests.RequestException as exc:
                raise RobotsFetchError("Не вдалося завантажити robots.txt. Перевірте адресу та спробуйте ще раз.") from exc

            try:
                _validate_connected_peer(response)
                if response.is_redirect or response.is_permanent_redirect:
                    location = response.headers.get("Location")
                    if not location:
                        raise RobotsFetchError("Сервер повернув редирект без нової адреси.")
                    next_url = urljoin(current_url, location)
                    _split_public_http_url(next_url)
                    current_url = next_url
                    redirects.append(current_url)
                    continue

                if response.status_code == 404:
                    raise RobotsFetchError(
                        "robots.txt не знайдено (HTTP 404). Для роботів це зазвичай "
                        "означає відсутність обмежень.",
                        url=current_url,
                        status_code=404,
                    )
                if not 200 <= response.status_code < 300:
                    raise RobotsFetchError(
                        f"Сервер повернув HTTP {response.status_code} замість robots.txt.",
                        url=current_url,
                        status_code=response.status_code,
                    )
                content_type = response.headers.get("Content-Type", "").lower()

                data = bytearray()
                truncated = False
                try:
                    for chunk in response.iter_content(chunk_size=16 * 1024):
                        if not chunk:
                            continue
                        remaining = MAX_ROBOTS_SIZE + 1 - len(data)
                        data.extend(chunk[:remaining])
                        if len(data) > MAX_ROBOTS_SIZE:
                            truncated = True
                            del data[MAX_ROBOTS_SIZE:]
                            break
                except requests.RequestException as exc:
                    raise RobotsFetchError("З'єднання перервано під час завантаження robots.txt.") from exc
                encoding_errors = False
                try:
                    content = bytes(data).decode("utf-8-sig")
                except UnicodeDecodeError:
                    encoding_errors = True
                    content = bytes(data).decode("utf-8-sig", errors="replace")
                return FetchResult(
                    content=content,
                    requested_url=requested_url,
                    final_url=current_url,
                    status_code=response.status_code,
                    size=len(data),
                    truncated=truncated,
                    redirects=tuple(redirects),
                    content_type=content_type,
                    encoding_errors=encoding_errors,
                )
            finally:
                response.close()
    finally:
        session.close()

    raise RobotsFetchError(f"Забагато редиректів — більше {MAX_REDIRECTS}.")
