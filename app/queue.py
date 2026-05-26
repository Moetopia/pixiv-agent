"""
asyncio 速率限制队列（令牌桶）。
用于对 Pixiv API 调用进行全局限速，防止被识别为恶意程序。
"""
import asyncio
import logging
import time
from typing import Any, Callable, Coroutine

from app.config import settings

logger = logging.getLogger(__name__)


class RateLimiter:
    """
    令牌桶速率限制器。
    关键修复：sleep 在锁外执行，避免阻塞其他调用方。
    使用虚拟时间推进：在锁内预算消耗时间偏移，保证并发正确性。
    """

    def __init__(self, rate: float):
        self._rate = rate  # tokens/second
        self._tokens = 1.0
        self._last = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        wait = 0.0
        async with self._lock:
            now = time.monotonic()
            elapsed = now - self._last
            self._tokens = min(1.0, self._tokens + elapsed * self._rate)
            self._last = now
            if self._tokens < 1.0:
                wait = (1.0 - self._tokens) / self._rate
                # 预算时间偏移：令后续 acquire 正确计算 elapsed
                self._last = now + wait
                self._tokens = 0.0
            else:
                self._tokens -= 1.0
        # sleep 在锁外执行
        if wait > 0:
            await asyncio.sleep(wait)


class CooldownState:
    """
    429 / Rate Limit 冷却状态。
    触发后所有 rate_limited_call 都会等待冷却结束。
    使用指数退避：连续 429 时等待时间翻倍，最长 1 小时。
    """

    def __init__(self):
        self._cooldown_until: float = 0.0
        self._consecutive_429: int = 0
        self._lock = asyncio.Lock()

    @property
    def is_cooling_down(self) -> bool:
        return time.monotonic() < self._cooldown_until

    @property
    def remaining_seconds(self) -> float:
        return max(0.0, self._cooldown_until - time.monotonic())

    async def notify_ratelimit(self, retry_after: int = 120) -> None:
        async with self._lock:
            self._consecutive_429 += 1
            base = max(retry_after, 60)
            wait = min(base * (2 ** (self._consecutive_429 - 1)), 3600)
            self._cooldown_until = time.monotonic() + wait
        logger.warning(
            f"[RateLimit] 429 冷却中 consecutive={self._consecutive_429}，等待 {wait:.0f}s"
        )

    async def notify_success(self) -> None:
        async with self._lock:
            self._consecutive_429 = 0

    async def wait_if_cooling(self) -> None:
        remaining = self.remaining_seconds
        if remaining > 0:
            logger.info(f"[RateLimit] 冷却中，等待 {remaining:.1f}s 后继续...")
            await asyncio.sleep(remaining)

    def status(self) -> dict:
        return {
            "is_cooling_down": self.is_cooling_down,
            "remaining_seconds": round(self.remaining_seconds, 1),
            "consecutive_429": self._consecutive_429,
        }


_rate_limiter = RateLimiter(rate=settings.RATE_LIMIT)
cooldown_state = CooldownState()


async def rate_limited_call(coro_func: Callable[[], Coroutine]) -> Any:
    """先等冷却，再等令牌，再执行。"""
    await cooldown_state.wait_if_cooling()
    await _rate_limiter.acquire()
    return await coro_func()


class SyncQueue:
    """
    简单 asyncio 队列：接收作者 ID，由后台 worker 顺序处理。
    通过 rate_limited_call 确保每个 Pixiv API 调用受速率限制。
    """

    def __init__(self):
        self._queue: asyncio.Queue[int] = asyncio.Queue()
        self._pending: set[int] = set()
        self._running: set[int] = set()
        self._lock = asyncio.Lock()

    @property
    def pending_count(self) -> int:
        return len(self._pending)

    @property
    def running_count(self) -> int:
        return len(self._running)

    async def enqueue(self, pixiv_user_id: int) -> bool:
        """将作者加入队列。若已在队列中或正在运行，返回 False。"""
        async with self._lock:
            if pixiv_user_id in self._pending or pixiv_user_id in self._running:
                return False
            self._pending.add(pixiv_user_id)
            await self._queue.put(pixiv_user_id)
            return True

    async def get(self) -> int:
        """从队列取出下一个作者 ID，标记为 running。"""
        pixiv_user_id = await self._queue.get()
        async with self._lock:
            self._pending.discard(pixiv_user_id)
            self._running.add(pixiv_user_id)
        return pixiv_user_id

    async def done(self, pixiv_user_id: int) -> None:
        async with self._lock:
            self._running.discard(pixiv_user_id)
        self._queue.task_done()

    def status(self) -> dict:
        return {
            "pending": self.pending_count,
            "running": self.running_count,
            "pending_ids": list(self._pending),
            "running_ids": list(self._running),
            "cooldown": cooldown_state.status(),
        }


sync_queue = SyncQueue()
