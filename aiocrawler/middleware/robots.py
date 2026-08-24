"""遵守 robots.txt。

默认开启。除了判断路径是否被允许，还会读取 robots.txt 中的 `Crawl-delay`
指令并写入 request.meta，由 ThrottleMiddleware 优先采用——站点自己声明的
抓取间隔比我们预设的值更该被尊重。

并发安全：同一站点的多个请求几乎同时到达时，用「缓存 Task 而非缓存结果」的
写法保证 robots.txt 只被下载一次，后到的请求 await 同一个 Task。

## 必须跟随全局网络配置

robots.txt 是本中间件自己发的请求，容易被忘掉：它同样要走 proxy、同样要遵守
verify_ssl。否则会出现「爬虫全程挂着代理，唯独每个站点的 robots.txt 从本机
直连出去」——每抓一个新站点就精确泄露一次真实 IP，而这恰恰是配代理要避免的。

响应体也要设上限。robots.txt 按规范只有几 KB，但对端想返回多少是它的自由，
不设限就等于给了对方一个把爬虫内存吃光的开关。
"""

from __future__ import annotations

from dataclasses import dataclass
from urllib.parse import urlsplit
from urllib.robotparser import RobotFileParser

import asyncio
import httpx
import structlog

from aiocrawler.exceptions import IgnoreRequest
from aiocrawler.middleware.base import Middleware
from aiocrawler.models import Request

log = structlog.get_logger(__name__)


@dataclass(slots=True)
class RobotsInfo:
    """一个站点的 robots.txt 解析结果。"""

    parser: RobotFileParser | None       # None 表示取不到 robots.txt，按全部允许处理
    crawl_delay: float | None = None


#: robots.txt 的体积上限。RFC 9309 建议至少解析 500 KiB，超出部分可以忽略
MAX_ROBOTS_BYTES = 512 * 1024


class RobotsMiddleware(Middleware):
    def __init__(
        self,
        *,
        user_agent: str = "*",
        timeout: float = 10.0,
        proxy: str | None = None,
        verify_ssl: bool = True,
    ) -> None:
        self._ua = user_agent
        self._timeout = timeout
        self._proxy = proxy
        self._verify_ssl = verify_ssl
        # origin -> 解析任务。存 Task 而不是存结果，才能让并发请求共享同一次下载
        self._cache: dict[str, asyncio.Task[RobotsInfo]] = {}
        self._client: httpx.AsyncClient | None = None

    async def open_spider(self, spider) -> None:
        self._client = httpx.AsyncClient(
            timeout=self._timeout,
            follow_redirects=True,
            headers={"User-Agent": self._ua if self._ua != "*" else "aiocrawler"},
            # 跟随全局配置，别让 robots 请求成为绕过代理的旁路
            proxy=self._proxy,
            verify=self._verify_ssl,
        )

    async def close_spider(self, spider) -> None:
        # 先把在途的 robots 请求取消，再关 client——否则会在它们脚下抽掉连接池
        for task in self._cache.values():
            if not task.done():
                task.cancel()
        self._cache.clear()
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    async def process_request(self, request: Request) -> None:
        parts = urlsplit(request.url)
        if parts.scheme not in ("http", "https"):
            return None

        origin = f"{parts.scheme}://{parts.netloc}"
        info = await self._get_info(origin)
        if info.parser is None:
            return None  # 取不到 robots.txt，按允许处理

        if not info.parser.can_fetch(self._ua, request.url):
            raise IgnoreRequest(f"robots.txt 禁止抓取该路径：{request.url}")

        # 把站点声明的抓取间隔透传给限速中间件
        if info.crawl_delay:
            request.meta.setdefault("_crawl_delay", info.crawl_delay)
        return None

    async def _get_info(self, origin: str) -> RobotsInfo:
        task = self._cache.get(origin)
        if task is None:
            # asyncio 单线程下，从取值到赋值之间不会被打断，这里不存在竞态
            task = asyncio.create_task(self._fetch(origin))
            self._cache[origin] = task
        try:
            return await task
        except Exception as exc:
            # 缓存的是 Task，异常会跟着一起被缓存下来——不清掉的话，该站点的
            # 每一个后续请求都会撞上同一个异常，整站永久抓不了
            self._cache.pop(origin, None)
            log.debug("robots_task_failed", origin=origin, error=str(exc))
            return RobotsInfo(parser=None)

    async def _fetch(self, origin: str) -> RobotsInfo:
        assert self._client is not None
        url = f"{origin}/robots.txt"
        try:
            text = await self._download(url)
        except Exception as exc:
            # 拿不到 robots.txt 时按「未禁止」处理，否则一次网络抖动就会让整站抓不了
            log.debug("robots_fetch_failed", url=url, error=str(exc))
            return RobotsInfo(parser=None)

        if text is None:
            return RobotsInfo(parser=None)

        parser = RobotFileParser()
        parser.parse(text.splitlines())
        delay = self._extract_crawl_delay(text, self._ua)
        log.debug("robots_loaded", origin=origin, crawl_delay=delay)
        return RobotsInfo(parser=parser, crawl_delay=delay)

    async def _download(self, url: str) -> str | None:
        """流式读取 robots.txt，超过上限即截断。None 表示「视为全部允许」。"""
        assert self._client is not None
        async with self._client.stream("GET", url) as resp:
            if resp.status_code >= 400:
                # 404 表示站点没有 robots.txt，按 RFC 9309 视为全部允许
                log.debug("robots_absent", url=url, status=resp.status_code)
                return None

            chunks: list[bytes] = []
            size = 0
            async for chunk in resp.aiter_bytes():
                chunks.append(chunk)
                size += len(chunk)
                if size >= MAX_ROBOTS_BYTES:
                    log.warning(
                        "robots_truncated", url=url, limit=MAX_ROBOTS_BYTES,
                        hint="robots.txt 超过体积上限，只解析前面的部分",
                    )
                    break
        raw = b"".join(chunks)[:MAX_ROBOTS_BYTES]
        return raw.decode("utf-8", errors="replace")

    @staticmethod
    def _extract_crawl_delay(text: str, ua: str) -> float | None:
        """自行解析 Crawl-delay，以支持小数值。

        标准库 RobotFileParser 用 `isdigit()` 校验该字段，因此 `Crawl-delay: 2.5`
        会被静默忽略——那样我们就会退回自己的默认间隔，反而比站点要求的更激进。
        这里补上小数支持。

        robots.txt 的分组规则：连续的 User-agent 行构成一组，其后的指令都属于
        这一组，直到出现下一个 User-agent 行为止。精确匹配的 UA 优先于通配 `*`。
        """
        groups: list[tuple[list[str], float | None]] = []
        agents: list[str] = []
        delay: float | None = None
        collecting_agents = True

        for raw in text.splitlines():
            line = raw.split("#", 1)[0].strip()
            if not line or ":" not in line:
                continue
            key, _, value = line.partition(":")
            key, value = key.strip().lower(), value.strip()

            if key == "user-agent":
                if not collecting_agents:
                    groups.append((agents, delay))
                    agents, delay = [], None
                    collecting_agents = True
                agents.append(value.lower())
            else:
                collecting_agents = False
                if key == "crawl-delay":
                    try:
                        delay = float(value)
                    except ValueError:
                        pass
        if agents:
            groups.append((agents, delay))

        ua_lower = ua.lower()
        wildcard: float | None = None
        for names, value in groups:
            if value is None:
                continue
            if any(n != "*" and n in ua_lower for n in names):
                return value          # 精确匹配优先
            if "*" in names and wildcard is None:
                wildcard = value
        return wildcard
