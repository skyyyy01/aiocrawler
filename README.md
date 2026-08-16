# aiocrawler

**[English](README.md)** | [中文](docs/README_ZH.md)

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

An async web crawling framework for Python, built from scratch. Scheduler,
downloader, middleware chain, pipeline and storage backends are all implemented
on top of `asyncio` + `httpx` — no Scrapy dependency.

**All six stages are complete and verified against real-world targets.**

| Capability | Implementation |
|---|---|
| Scheduling | In-memory / SQLite-persistent / Redis-distributed, all behind one interface |
| Downloading | HTTP (httpx) and browser rendering (Playwright), routed per request |
| Middleware | Retry, per-domain throttling, robots.txt, UA rotation, proxy pool |
| Storage | JSONL / CSV / SQLite / PostgreSQL / MySQL / MongoDB |
| Engineering | Four-layer config merge, resumable crawls, graceful shutdown, CLI |

---

## Quick start

```bash
# Install (optional groups as needed)
.venv/bin/pip install -e ".[dev,browser,sqlite,postgres,mysql,mongo]"
.venv/bin/playwright install chromium        # only for browser rendering

.venv/bin/python -m aiocrawler list          # list spiders
.venv/bin/python -m aiocrawler run books     # static site, ~35s for 1000 items
.venv/bin/python -m aiocrawler run quotes_js # JS-rendered site, 100 items
```

Common options:

```bash
-n 20                     # scrape only 20 items (for debugging)
-o out/x.csv              # change output format
-o "postgresql://u:p@h/db"  # write straight to a database
-c 4 --delay 0.5          # concurrency and throttling
--resume                  # resumable crawl, pick up where it stopped
--redis redis://host:6379/0  # distributed: shared queue across nodes
```

Tests:

```bash
.venv/bin/python -m pytest -q          # 188 pass; those needing services skip themselves
.venv/bin/python -m pytest -m "not browser"   # skip cases requiring Chromium
```

External services are wired in through environment variables. Leave them unset
and the corresponding cases skip automatically:

```bash
AIOCRAWLER_TEST_PG=postgresql://postgres:pass@127.0.0.1:5432/crawl
AIOCRAWLER_TEST_MYSQL=mysql://root:pass@127.0.0.1:3306/crawl
AIOCRAWLER_TEST_MONGO=mongodb://127.0.0.1:27017
AIOCRAWLER_TEST_REDIS=redis://127.0.0.1:6379/0
```

---

## Writing a spider

Drop it into `spiders/` and it is discovered automatically:

```python
from aiocrawler import BaseSpider, Item, Response

class MyItem(Item):
    title: str
    price: float

class MySpider(BaseSpider):
    name = "my"
    start_urls = ["https://example.com/list"]
    renderer = "http"                      # switch to "browser" if the whole site needs JS
    custom_settings = {"concurrency": 8, "download_delay": 0.5}

    async def parse(self, response: Response):
        for a in response.css("a.item"):
            # higher priority dequeues first; a positive value on detail pages
            # keeps the queue from piling up
            yield response.follow(a.attributes["href"], callback="parse_detail", priority=1)

    async def parse_detail(self, response: Response):
        yield MyItem(title=response.text_of("h1"),
                     price=float(response.text_of("span.price").lstrip("¥")))
```

`parse` is an async generator: `yield` an `Item` to send it down the pipeline for
storage, `yield` a `Request` to keep crawling. To render one single request in a
browser, write `Request(url, renderer="browser")`; `follow()` inherits the current
page's renderer by default.

---

## Data flow

```
Spider.start_requests()  ─→  Request
                                │
                   ┌────────────▼─────────────┐
                   │ Scheduler                │  priority queue + fingerprint dedup
                   │  Memory / SQLite / Redis │  three impls, one interface
                   └────────────┬─────────────┘
                                │  Engine's worker pool pulls up to the concurrency cap
                   ┌────────────▼─────────────┐
                   │ Middleware: request side │  retry→robots→UA→proxy→throttle
                   └────────────┬─────────────┘
                   ┌────────────▼─────────────┐
                   │ DownloaderRouter         │  dispatches on request.renderer
                   │   ├ HttpDownloader       │  httpx (default)
                   │   └ BrowserDownloader    │  Playwright (started lazily)
                   └────────────┬─────────────┘
                                ▼  Response
                   ┌──────────────────────────┐
                   │ Middleware: response side│  retry decision / exception fallback
                   └────────────┬─────────────┘
                   ┌────────────▼─────────────┐
                   │ Spider.parse(response)   │  the only code you have to write
                   └───────┬───────────┬──────┘
                      Item │           │  Request ──→ back to the Scheduler
                   ┌───────▼──────────────────┐
                   │ Pipeline (batch buffer)  │
                   └────────────┬─────────────┘
                   ┌────────────▼─────────────┐
                   │ Storage backend          │  files / SQL / document stores
                   └──────────────────────────┘
```

---

## Code tour

Reading in this order is the easiest way in:

| File | Responsibility | Key point |
|---|---|---|
| `models.py` | Request / Response / Item | **`callback` holds a method name string**, which keeps Request JSON-serializable |
| `scheduler/base.py` | Scheduler interface | The most important abstraction boundary in the framework; `ack` is what makes the queue reliable |
| `scheduler/memory.py` | In-memory impl | heapq with negated values for a max-heap; a monotonic counter prevents ever comparing Requests |
| `scheduler/sqlite.py` | Persistent impl | pending/inflight two-state model; un-acked requests come back after a restart |
| `scheduler/redis_backend.py` | Distributed impl | Lua makes pop atomic; a visibility timeout separates "alive" from "dead" |
| `middleware/base.py` | Middleware chain | Onion structure: requests in order, responses in reverse |
| `middleware/retry.py` | Retry | Exponential backoff with jitter; **retried requests must set `dont_filter`** |
| `middleware/throttle.py` | Throttling | **Per-domain**, not one globally shared rate |
| `downloader/router.py` | Hybrid dispatch | Browser starts lazily, so pure-HTTP crawls pay nothing |
| `downloader/browser.py` | Rendering | One Browser plus a context pool; images and fonts blocked by default |
| `engine.py` | Main loop | Termination detection; `_inflight` must be decremented *after* new requests are enqueued |
| `pipeline/storage.py` | Batching | Flushes at 200 items or 5 seconds; must flush before closing |
| `storage/` | Six backends | Uniform `open/write(batch)/close` |

---

## Seven key design decisions

1. **`callback` stores a string, not a function reference** (`models.py`)
   This makes `Request` pure data and JSON-serializable, which is the prerequisite
   for going distributed. Scrapy pickles closures instead, a long-standing source
   of pain in its distributed setups.

2. **Termination has to look at the in-flight count** (`engine.py`)
   An empty queue does not mean the crawl is done — a worker may be parsing right
   now and about to emit new links. And `_inflight` must be decremented *after*
   new requests are enqueued; getting that order wrong drops data silently.

3. **Throttling is per-domain** (`middleware/throttle.py`)
   A globally shared rate lets one slow site eat everyone else's quota.

4. **Retried requests must carry `dont_filter`** (`middleware/retry.py`)
   Otherwise their fingerprint matches the original and the dedup filter drops
   them silently — which shows up as "retries do nothing at all", with no error
   anywhere.

5. **Middleware order: Retry first, Throttle last** (`middleware/__init__.py`)
   The response/exception side runs in reverse, so putting Retry first means it
   runs last. That is what gives Proxy a chance to mark a dead proxy into cooldown
   first, so the retry can pick a healthy one.

6. **Browser reuse plus lazy startup** (`downloader/`)
   One Browser process for the whole run with a fixed-size context pool; if no
   request needs a browser, Chromium never starts at all.

7. **A visibility timeout separates "alive" from "dead"** (`scheduler/redis_backend.py`)
   Reclaiming every leftover inflight entry on restart is safe on a single machine,
   but doing the same thing when distributed steals requests other nodes are still
   working on. See the verification log below.

---

## Verification log

Every stage was verified against real targets, not just stubs.

**Full crawl of a static site** (`books.toscrape.com`)
```
1050 requests | 1050 responses | 0 failures | 1000 items | 34.5s
```
1050 requests = 50 listing pages + 1000 detail pages, exactly matching the site
structure; all 1000 records have unique URLs and UPCs, spanning 50 categories.

**Hybrid downloader A/B** (`quotes.toscrape.com/js`, content generated purely by JS)
```
Same URL, same 200 response, only renderer changed:
  renderer='http'     →   0 items
  renderer='browser'  →  10 items
```

**Consistency across four database backends**: the same batch written to SQLite /
PostgreSQL / MySQL / MongoDB and read back matches exactly; re-running leaves the
count unchanged, so UPSERT is idempotent. PG/MySQL/Mongo were tested against
throwaway Docker containers, not mocks.

**Graceful shutdown**: with the batch threshold set to 100000 (far above the crawl
size), SIGINT after 6 seconds still left 48 rows in the file — proof the buffer
really is flushed and an interrupt loses nothing.

**Resumable crawl**: interrupted, then continued with `--resume`; 74 rows total
across both runs with 0 duplicates, so the dedup table survives across processes.

**Distributed**: two independent processes sharing a Redis queue crawled the whole site
```
node A 509 items + node B 491 items = 1000 items, 0 overlap
1050 total requests = 1050 total responses (nothing downloaded twice)
```

### A real defect exposed by stage 6

The first distributed run produced **7 overlapping fetches**. The root cause was
crash-recovery logic that treated every inflight entry as "leftovers from the last
crash" — but when a new node joins, those entries actually belong to a **node that
is still running**. The logic is correct for a single machine (nothing else is
alive during a restart); carried over to a distributed setting it became a bug.

The fix was a visibility timeout: inflight entries record when they were taken
(timestamps all come from the Redis server's `TIME` to avoid clock skew between
nodes), and only entries stuck past the threshold get reclaimed. Overlap dropped
to zero afterwards.

This is exactly why stage 6 exists — using a real distributed implementation to
test the abstraction in reverse. The conclusion: **the `BaseScheduler` interface
itself held up** (all three implementations pass the same contract tests with zero
changes), but **the engine's termination policy** needed an `idle_timeout` for the
distributed case. A shared queue being momentarily empty is normal — another node
is busy parsing a page — so you cannot call it a day the moment you see an empty
queue.

---

## Tests

```
206 cases (with all external services available) / 188 (clean environment, the rest skip)
```

| File | Coverage |
|---|---|
| `test_models.py` | URL normalization, fingerprints, serialization round-trips |
| `test_scheduler.py` / `test_scheduler_sqlite.py` | Priority, dedup, ack, crash recovery |
| `test_scheduler_contract.py` | **One set of assertions run against all three schedulers** |
| `test_scheduler_redis.py` | Visibility timeout, work splitting across nodes |
| `test_middleware.py` | Chain order, retry, throttling, robots, proxy |
| `test_downloader.py` | Route dispatch; real Chromium rendering |
| `test_engine.py` | Termination detection, dedup, failure isolation |
| `test_integration.py` | Retry and throttling over real TCP |
| `test_storage*.py` | Six backends, type inference, UPSERT |
| `test_settings.py` | Four-layer config merge |

The contract tests carry the most weight here: if any scheduler implementation
needed special treatment to pass, that would mean the abstraction is wrong.

---

## Configuration

Lowest to highest precedence: built-in defaults < `[default]` in `settings.toml` <
a spider's `custom_settings` < `[spider.<name>]` in `settings.toml` < command line.

Layers 3 and 4 are in that order deliberately: `custom_settings` is the default the
spider author wrote into the code, while the dedicated section in the toml belongs
to the operations side — adjusting production behavior should not require a code change.

```toml
[default]
concurrency = 16
download_delay = 1.0      # conservative by default
respect_robots = true

[spider.books]
concurrency = 8
download_delay = 0.1
```

> robots.txt is respected by default, with a 1 second per-domain delay. The example
> spiders relax that to 0.1s for `toscrape.com` (a site built specifically for
> scraping practice, with no robots.txt restrictions); keep the conservative
> defaults when crawling production sites.
