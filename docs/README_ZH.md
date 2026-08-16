# aiocrawler

[English](../README.md) | **[中文](README_ZH.md)**

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](../LICENSE)

自研 Python 异步爬虫框架。基于 `asyncio` + `httpx` 从零实现调度器、下载器、
中间件链、管道与存储后端——不依赖 Scrapy。

**六个阶段全部完成并经过真实环境验证。**

| 能力 | 实现 |
|---|---|
| 调度 | 内存 / SQLite 持久化 / Redis 分布式，三者满足同一接口 |
| 下载 | HTTP（httpx）与浏览器渲染（Playwright）混合，按请求路由 |
| 中间件 | 重试、按域名限速、robots.txt、UA 轮换、代理池 |
| 存储 | JSONL / CSV / SQLite / PostgreSQL / MySQL / MongoDB |
| 工程化 | 配置四层合并、断点续爬、优雅退出、CLI |

---

## 快速开始

```bash
# 安装（按需选装可选组）
.venv/bin/pip install -e ".[dev,browser,sqlite,postgres,mysql,mongo]"
.venv/bin/playwright install chromium        # 仅浏览器渲染需要

.venv/bin/python -m aiocrawler list          # 列出爬虫
.venv/bin/python -m aiocrawler run books     # 静态站，约 35 秒 / 1000 条
.venv/bin/python -m aiocrawler run quotes_js # JS 渲染站，100 条
```

常用参数：

```bash
-n 20                     # 只抓 20 条（调试用）
-o out/x.csv              # 换输出格式
-o "postgresql://u:p@h/db"  # 直接写数据库
-c 4 --delay 0.5          # 并发与限速
--resume                  # 断点续爬，中断后接着跑
--redis redis://host:6379/0  # 分布式：多节点共享队列
```

测试：

```bash
.venv/bin/python -m pytest -q          # 188 通过，需外部服务的自动跳过
.venv/bin/python -m pytest -m "not browser"   # 跳过需要 Chromium 的用例
```

外部服务通过环境变量接入，未设置则相关用例自动跳过：

```bash
AIOCRAWLER_TEST_PG=postgresql://postgres:pass@127.0.0.1:5432/crawl
AIOCRAWLER_TEST_MYSQL=mysql://root:pass@127.0.0.1:3306/crawl
AIOCRAWLER_TEST_MONGO=mongodb://127.0.0.1:27017
AIOCRAWLER_TEST_REDIS=redis://127.0.0.1:6379/0
```

---

## 写一个爬虫

放进 `spiders/` 即被自动发现：

```python
from aiocrawler import BaseSpider, Item, Response

class MyItem(Item):
    title: str
    price: float

class MySpider(BaseSpider):
    name = "my"
    start_urls = ["https://example.com/list"]
    renderer = "http"                      # 整站需要 JS 渲染就改成 "browser"
    custom_settings = {"concurrency": 8, "download_delay": 0.5}

    async def parse(self, response: Response):
        for a in response.css("a.item"):
            # priority 越大越优先出队；详情页给正数可避免队列堆积
            yield response.follow(a.attributes["href"], callback="parse_detail", priority=1)

    async def parse_detail(self, response: Response):
        yield MyItem(title=response.text_of("h1"),
                     price=float(response.text_of("span.price").lstrip("¥")))
```

`parse` 是 async generator：`yield` 出 `Item` 进管道存盘，`yield` 出 `Request` 继续抓。
单个请求要用浏览器渲染时写 `Request(url, renderer="browser")`，`follow()` 默认继承当前页的渲染方式。

---

## 数据流

```
Spider.start_requests()  ─→  Request
                                │
                    ┌───────────▼────────────┐
                    │ Scheduler              │  优先级队列 + 指纹去重
                    │  内存 / SQLite / Redis │  三者同一接口
                    └───────────┬────────────┘
                                │  Engine 的 worker 池按并发上限拉取
                    ┌───────────▼────────────┐
                    │ 中间件请求侧（正序）   │  重试→robots→UA→代理→限速
                    └───────────┬────────────┘
                    ┌───────────▼────────────┐
                    │ DownloaderRouter       │  按 request.renderer 分发
                    │   ├ HttpDownloader     │  httpx（默认）
                    │   └ BrowserDownloader  │  Playwright（惰性启动）
                    └───────────┬────────────┘
                                ▼  Response
                    ┌────────────────────────┐
                    │ 中间件响应侧（逆序）   │  重试判定 / 异常兜底
                    └───────────┬────────────┘
                    ┌───────────▼────────────┐
                    │ Spider.parse(response) │  你唯一要写的代码
                    └──────┬──────────┬──────┘
                     Item  │          │  Request ──→ 回到 Scheduler
                    ┌──────▼──────────────────┐
                    │ Pipeline（攒批缓冲）    │
                    └───────────┬─────────────┘
                    ┌───────────▼─────────────┐
                    │ Storage backend         │  文件 / SQL / 文档库
                    └─────────────────────────┘
```

---

## 代码导览

按此顺序读最易理解：

| 文件 | 职责 | 关键点 |
|---|---|---|
| `models.py` | Request / Response / Item | **`callback` 存方法名字符串**，使 Request 可 JSON 序列化 |
| `scheduler/base.py` | 调度器接口 | 全框架最重要的抽象边界；`ack` 支撑可靠队列 |
| `scheduler/memory.py` | 内存实现 | heapq 取负值实现大顶堆；自增序号避免比较到 Request |
| `scheduler/sqlite.py` | 持久化实现 | pending/inflight 两态，未 ack 的请求重启后复活 |
| `scheduler/redis_backend.py` | 分布式实现 | Lua 保证 pop 原子；可见性超时区分「活跃」与「已死」 |
| `middleware/base.py` | 中间件链 | 请求正序、响应逆序的洋葱结构 |
| `middleware/retry.py` | 重试 | 指数退避 + 抖动；**重试请求必须 `dont_filter`** |
| `middleware/throttle.py` | 限速 | **按域名独立**，不是全局共享速率 |
| `downloader/router.py` | 混合分发 | 浏览器惰性启动，纯 HTTP 爬虫零开销 |
| `downloader/browser.py` | 渲染 | 单 Browser + context 池；默认拦截图片字体 |
| `engine.py` | 主循环 | 结束判定；`_inflight` 递减必须在新请求入队之后 |
| `pipeline/storage.py` | 攒批 | 满 200 条或超 5 秒落盘；关闭前必须 flush |
| `storage/` | 六种后端 | 统一 `open/write(batch)/close` |

---

## 七个关键设计决策

1. **`callback` 存字符串而非函数引用**（`models.py`）
   使 `Request` 成为纯数据、可 JSON 序列化，这是分布式升级的前提。
   Scrapy 用 pickle 序列化闭包，是其分布式方案长期的痛点来源。

2. **结束判定要看在途计数**（`engine.py`）
   队列空 ≠ 爬完了，可能有 worker 正在解析、马上产出新链接。且 `_inflight`
   递减必须发生在新请求入队之后，顺序颠倒会静默丢数据。

3. **限速按域名独立**（`middleware/throttle.py`）
   全局共享速率会让一个慢站点拖垮所有站点的配额。

4. **重试请求必须带 `dont_filter`**（`middleware/retry.py`）
   否则指纹与原请求相同，会被去重器静默丢弃——表现为「重试完全不生效」
   且没有任何报错。

5. **中间件顺序：Retry 最前、Throttle 最后**（`middleware/__init__.py`）
   响应/异常侧逆序执行，Retry 放最前意味着它最后执行，这样 Proxy 才有机会
   先把失效代理标记进冷却，重试时才能选到健康代理。

6. **浏览器复用 + 惰性启动**（`downloader/`）
   全程一个 Browser 进程、固定数量 context 池；没有 browser 请求时
   Chromium 根本不启动。

7. **可见性超时区分「活跃」与「已死」**（`scheduler/redis_backend.py`）
   单机重启时回收全部残留 inflight 是安全的，分布式下这样做会抢走其他
   节点正在处理的请求。详见下方验证记录。

---

## 验证记录

各阶段均在真实环境下验证，而非仅靠打桩测试。

**静态站全量抓取**（`books.toscrape.com`）
```
请求 1050 个 | 响应 1050 个 | 失败 0 个 | 产出 1000 条 | 耗时 34.5s
```
请求数 1050 = 50 个列表页 + 1000 个详情页，与站点结构完全吻合；
1000 条记录的 URL 与 UPC 均唯一，覆盖 50 个分类。

**混合下载器对照**（`quotes.toscrape.com/js`，内容纯由 JS 生成）
```
同一 URL、同样 200 响应，只改 renderer：
  renderer='http'     →   0 条
  renderer='browser'  →  10 条
```

**四种数据库后端一致性**：同一批数据分别写入 SQLite / PostgreSQL / MySQL /
MongoDB，读回比对完全一致；重跑一次条数不变，UPSERT 幂等。
PG/MySQL/Mongo 用临时 Docker 容器实测，非 mock。

**优雅退出**：把攒批阈值设为 100000（远大于抓取量），抓 6 秒后发 SIGINT，
文件仍有 48 条——证明缓冲确实被刷盘，中断不丢数据。

**断点续爬**：中断后 `--resume` 继续，两轮合计 74 行、0 重复，
去重表跨进程复用。

**分布式**：两个独立进程共享 Redis 队列抓完全站
```
节点 A 509 条 + 节点 B 491 条 = 1000 条，重叠 0 条
总请求 1050 = 总响应 1050（无任何重复下载）
```

### 阶段 6 暴露的一个真实缺陷

首次分布式验证出现 **7 条重叠抓取**。根因是崩溃恢复逻辑把所有 inflight
一律当作「上次崩溃的残留」回收——而新节点加入时，那些条目其实归**正在运行的
节点**所有。单机场景下这个逻辑是对的（重启时没有其他活跃进程），照搬到分布式
就成了 bug。

修复方式是引入可见性超时：inflight 记录取出时刻（时间戳统一取自 Redis 服务端
`TIME`，避免节点间时钟偏差），只回收滞留超过阈值的条目。修复后重叠归零。

这正是阶段 6 存在的意义——用一个真实的分布式实现去反向检验抽象。结论是：
**`BaseScheduler` 接口本身经受住了检验**（三种实现零改动通过同一组契约测试），
但**引擎的结束判定策略**需要为分布式补一个 `idle_timeout`：共享队列瞬时为空是
常态（别的节点正在解析页面），不能一看到空就收工。

---

## 测试

```
206 个用例（外部服务齐备时）/ 188 个（纯净环境，其余自动跳过）
```

| 文件 | 覆盖 |
|---|---|
| `test_models.py` | URL 规范化、指纹、序列化往返 |
| `test_scheduler.py` / `test_scheduler_sqlite.py` | 优先级、去重、ack、崩溃恢复 |
| `test_scheduler_contract.py` | **同一组断言跑在三种调度器上** |
| `test_scheduler_redis.py` | 可见性超时、多节点分工 |
| `test_middleware.py` | 链顺序、重试、限速、robots、代理 |
| `test_downloader.py` | 路由分发；真实 Chromium 渲染 |
| `test_engine.py` | 结束判定、去重、故障隔离 |
| `test_integration.py` | 真实 TCP 上的重试与限速 |
| `test_storage*.py` | 六种后端、类型推断、UPSERT |
| `test_settings.py` | 配置四层合并 |

契约测试是这套测试里分量最重的一个：如果某个调度器实现需要特殊对待才能通过，
就说明抽象没做对。

---

## 配置

优先级由低到高：内置默认 < `settings.toml` 的 `[default]` <
spider 的 `custom_settings` < `settings.toml` 的 `[spider.<名字>]` < 命令行。

第 3 层与第 4 层的先后是刻意的：`custom_settings` 是爬虫作者写在代码里的默认值，
而 toml 里的专属段属于运维侧调整，调线上行为不必改代码。

```toml
[default]
concurrency = 16
download_delay = 1.0      # 默认偏保守
respect_robots = true

[spider.books]
concurrency = 8
download_delay = 0.1
```

> 默认开启 robots.txt 遵守，默认间隔 1 秒/域名。示例爬虫针对
> `toscrape.com`（专为爬虫练习搭建、无 robots.txt 限制）放宽到 0.1 秒；
> 抓取生产站点时建议保留保守默认值。
