"""Redis 分布式调度器。

**这个类存在的首要目的，是反向检验阶段 1 定下的 BaseScheduler 抽象是否真的够用。**
如果为了接上 Redis 而不得不修改引擎，就说明当初的抽象有漏洞。实际结果是：
引擎与 spider 代码一行未改。

## 数据结构

    {prefix}:queue     ZSET   待处理队列，score 编码「优先级 + 先进先出」
    {prefix}:inflight  HASH   已取出未确认，member -> 原 score（崩溃恢复用）
    {prefix}:seen      SET    去重指纹
    {prefix}:seq       INT    自增序号，用于 FIFO 与成员唯一

## score 的编码

    score = -priority * 10^12 + seq

ZPOPMIN 取最小值，因此优先级越高（priority 越大）score 越小、越先出队；
同优先级则按 seq 递增，即先进先出。10^12 的量级保证 seq 不会串到优先级位。

## 成员唯一性

ZSET 的成员必须互不相同，而 dont_filter 的请求允许重复入队（重试就是这种情况）。
因此 member 拼成 `seq|json`，由自增序号保证唯一。

## 可靠队列与可见性超时

pop 把成员从 queue 移入 inflight，ack 才真正删除。pop 的「弹出 + 记账」必须
原子，否则并发消费者会拿到同一条——用 Lua 脚本保证。

**关键点：崩溃恢复不能无差别回收所有 inflight。** 单机版（SqliteScheduler）
重启时不存在其他活跃进程，把残留的 inflight 全部放回队列是安全的。但分布式
不同：新节点加入时，其他节点正有一批请求在处理中，若把它们全部回收，这些请求
就会被重复抓取一遍。

因此这里采用可见性超时（visibility timeout，与 SQS 的机制同理）：inflight 记录
取出时刻，只有滞留超过 `visibility_timeout` 的条目才被判定为「持有者已死」并回收。
时间戳统一取自 Redis 服务端的 TIME 命令，避免各节点时钟偏差造成误判。
"""

from __future__ import annotations

from typing import Any

import structlog

from aiocrawler.models import Request

log = structlog.get_logger(__name__)

#: 优先级在 score 中占据的量级，须远大于可能的 seq 取值
_PRIORITY_SCALE = 10 ** 12

# 弹出最小 score 的成员，同时以 "score:取出时刻" 记入 inflight。原子执行。
_POP_LUA = """
local popped = redis.call('ZPOPMIN', KEYS[1], 1)
if #popped == 0 then
    return false
end
local member = popped[1]
local score  = popped[2]
redis.call('HSET', KEYS[2], member, score .. ':' .. ARGV[1])
return member
"""

# 只回收滞留超过可见性超时的 inflight 条目。
# 其余条目视为仍被活跃节点持有，不能动——否则会造成重复抓取。
_RECOVER_LUA = """
local now     = tonumber(ARGV[1])
local timeout = tonumber(ARGV[2])
local entries = redis.call('HGETALL', KEYS[2])
local n = 0
for i = 1, #entries, 2 do
    local member = entries[i]
    local value  = entries[i + 1]
    local sep    = string.find(value, ':', 1, true)
    if sep then
        local score = tonumber(string.sub(value, 1, sep - 1))
        local taken = tonumber(string.sub(value, sep + 1))
        if score and taken and (now - taken) > timeout then
            redis.call('ZADD', KEYS[1], score, member)
            redis.call('HDEL', KEYS[2], member)
            n = n + 1
        end
    end
end
return n
"""


class RedisScheduler:
    def __init__(
        self,
        url: str = "redis://127.0.0.1:6379/0",
        *,
        prefix: str = "aiocrawler",
        resume: bool = True,
        visibility_timeout: float = 300.0,
    ) -> None:
        """
        :param visibility_timeout: inflight 条目滞留多久后判定持有者已死并回收。
            必须**明显大于**单个请求的最长处理耗时（含重试退避），否则会把正常
            处理中的请求误判为失效，造成重复抓取。
        """
        self._url = url
        self._prefix = prefix
        self._resume = resume
        self._visibility_timeout = visibility_timeout
        self._redis: Any = None
        self._pop_script: Any = None
        self._recover_script: Any = None

    async def _now(self) -> float:
        """取 Redis 服务端时间，保证多节点使用同一时钟。"""
        seconds, microseconds = await self._redis.time()
        return seconds + microseconds / 1_000_000

    # ---- key 命名 ----
    @property
    def _k_queue(self) -> str:
        return f"{self._prefix}:queue"

    @property
    def _k_inflight(self) -> str:
        return f"{self._prefix}:inflight"

    @property
    def _k_seen(self) -> str:
        return f"{self._prefix}:seen"

    @property
    def _k_seq(self) -> str:
        return f"{self._prefix}:seq"

    async def open(self) -> None:
        from redis.asyncio import Redis

        self._redis = Redis.from_url(self._url, decode_responses=True)
        self._pop_script = self._redis.register_script(_POP_LUA)
        self._recover_script = self._redis.register_script(_RECOVER_LUA)

        if not self._resume:
            await self._redis.delete(
                self._k_queue, self._k_inflight, self._k_seen, self._k_seq
            )
            log.info("redis_scheduler_cleared", prefix=self._prefix)
            return

        requeued = await self._reclaim_stale()
        pending = await self.size()
        seen = await self.count()
        if pending or seen:
            log.info(
                "redis_scheduler_restored",
                pending=pending,
                reclaimed=requeued,
                known_fingerprints=seen,
                note="只回收滞留超时的条目，其余仍归活跃节点所有",
            )

    async def _reclaim_stale(self) -> int:
        """回收被已死节点持有的请求。活跃节点正在处理的不受影响。"""
        now = await self._now()
        n = await self._recover_script(
            keys=[self._k_queue, self._k_inflight],
            args=[now, self._visibility_timeout],
        )
        return int(n or 0)

    # ---- 去重 ----

    async def seen(self, fingerprint: str) -> bool:
        # SADD 返回新增元素个数：0 表示此前已存在
        added = await self._redis.sadd(self._k_seen, fingerprint)
        return added == 0

    async def count(self) -> int:
        return int(await self._redis.scard(self._k_seen))

    # ---- 队列 ----

    async def push(self, request: Request) -> bool:
        if not request.dont_filter:
            if await self.seen(request.fingerprint()):
                return False

        seq = int(await self._redis.incr(self._k_seq))
        score = -request.priority * _PRIORITY_SCALE + seq
        member = f"{seq}|{request.to_json()}"
        await self._redis.zadd(self._k_queue, {member: score})
        return True

    async def pop(self) -> Request | None:
        member = await self._pop_script(
            keys=[self._k_queue, self._k_inflight], args=[await self._now()]
        )
        if not member:
            return None

        _, _, payload = str(member).partition("|")
        request = Request.from_json(payload)
        # 记住原始 member，ack 时据此从 inflight 中删除
        request.meta["_redis_member"] = member
        return request

    async def ack(self, request: Request) -> None:
        member = request.meta.pop("_redis_member", None)
        if member is None:
            return
        await self._redis.hdel(self._k_inflight, member)

    async def size(self) -> int:
        return int(await self._redis.zcard(self._k_queue))

    async def inflight(self) -> int:
        return int(await self._redis.hlen(self._k_inflight))

    async def close(self) -> None:
        if self._redis is not None:
            await self._redis.aclose()
            self._redis = None
