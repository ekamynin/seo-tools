import asyncio
import aiohttp
import base64
from dataclasses import dataclass
from typing import Optional, Callable


@dataclass
class CheckResult:
    url: str
    indexed: Optional[bool] = None
    error: Optional[str] = None


class DataForSEOChecker:
    BASE_URL = "https://api.dataforseo.com/v3/serp/google/organic/live/regular"

    def __init__(self, login: str, password: str, concurrency: int = 5):
        self.login = login
        self.password = password
        self.concurrency = concurrency

    def _get_headers(self):
        creds = base64.b64encode(f"{self.login}:{self.password}".encode()).decode()
        return {
            "Authorization": f"Basic {creds}",
            "Content-Type": "application/json",
        }

    async def _check_one(self, session: aiohttp.ClientSession, url: str) -> CheckResult:
        payload = [{
            "keyword": f"site:{url}",
            "location_code": 2804,
            "language_code": "uk",
            "depth": 10,
        }]
        try:
            timeout = aiohttp.ClientTimeout(total=30)
            async with session.post(self.BASE_URL, json=payload, headers=self._get_headers(), timeout=timeout) as resp:
                try:
                    data = await resp.json(content_type=None)
                except Exception:
                    return CheckResult(url=url, error=f"Невалідна відповідь API (HTTP {resp.status})")
                if data.get("status_code") != 20000:
                    if data.get("status_code") == 40203:
                        return CheckResult(url=url, error="Денний ліміт API вичерпано. Спробуйте завтра.")
                    return CheckResult(url=url, error=data.get("status_message", "API error"))
                tasks = data.get("tasks") or []
                if not tasks:
                    return CheckResult(url=url, error="Порожня відповідь tasks")
                task = tasks[0]
                task_code = task.get("status_code")
                task_msg  = task.get("status_message", "")
                if task_code != 20000:
                    if "no search results" in task_msg.lower():
                        return CheckResult(url=url, indexed=False)
                    return CheckResult(url=url, error=task_msg)
                result = (task.get("result") or [{}])[0]
                items_count = result.get("items_count", 0)
                return CheckResult(url=url, indexed=items_count > 0)
        except asyncio.TimeoutError:
            return CheckResult(url=url, error="Timeout (30s)")
        except Exception as e:
            return CheckResult(url=url, error=str(e))

    async def check_urls(
        self,
        urls: list[str],
        progress_callback: Optional[Callable[[int, int], None]] = None,
    ) -> list[CheckResult]:
        semaphore = asyncio.Semaphore(self.concurrency)
        results: list[CheckResult] = []

        async def bounded(url: str):
            async with semaphore:
                result = await self._check_one(session, url)
            results.append(result)
            if progress_callback:
                progress_callback(len(results), len(urls))

        async with aiohttp.ClientSession() as session:
            await asyncio.gather(*[bounded(u) for u in urls])

        order = {url: i for i, url in enumerate(urls)}
        results.sort(key=lambda r: order.get(r.url, 0))
        return results


class SerpAPIChecker:
    BASE_URL = "https://serpapi.com/search"

    def __init__(self, api_key: str, concurrency: int = 5):
        self.api_key = api_key
        self.concurrency = concurrency

    async def _check_one(self, session: aiohttp.ClientSession, url: str) -> CheckResult:
        params = {
            "engine": "google",
            "q": f"site:{url}",
            "api_key": self.api_key,
            "num": 10,
        }
        try:
            async with session.get(self.BASE_URL, params=params) as resp:
                data = await resp.json()
                if "error" in data:
                    return CheckResult(url=url, error=data["error"])
                organic = data.get("organic_results", [])
                return CheckResult(url=url, indexed=len(organic) > 0)
        except Exception as e:
            return CheckResult(url=url, error=str(e))

    async def check_urls(
        self,
        urls: list[str],
        progress_callback: Optional[Callable[[int, int], None]] = None,
    ) -> list[CheckResult]:
        semaphore = asyncio.Semaphore(self.concurrency)
        results: list[CheckResult] = []

        async def bounded(url: str):
            async with semaphore:
                result = await self._check_one(session, url)
            results.append(result)
            if progress_callback:
                progress_callback(len(results), len(urls))

        async with aiohttp.ClientSession() as session:
            await asyncio.gather(*[bounded(u) for u in urls])

        order = {url: i for i, url in enumerate(urls)}
        results.sort(key=lambda r: order.get(r.url, 0))
        return results
