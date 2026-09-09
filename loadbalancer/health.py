"""Active health checking: periodically GET a path on each backend and flip its
``health`` between UP and DOWN. Runs as an asyncio background task.
"""

from __future__ import annotations

import asyncio
import time

from .backends import Backend, BackendPool, Health
from .http1 import read_response


class HealthChecker:
    def __init__(self, pool: BackendPool, path: str = "/health", *,
                 interval: float = 1.0, timeout: float = 1.0,
                 rise: int = 1, fall: int = 2):
        self.pool = pool
        self.path = path
        self.interval = interval
        self.timeout = timeout
        self.rise = rise            # consecutive OKs to mark UP
        self.fall = fall            # consecutive fails to mark DOWN
        self._up: dict[str, int] = {}
        self._down: dict[str, int] = {}
        self._task: asyncio.Task | None = None

    def start(self) -> None:
        self._task = asyncio.ensure_future(self._loop())

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    async def check_once(self) -> None:
        await asyncio.gather(*(self._probe(b) for b in self.pool.backends),
                             return_exceptions=True)

    async def _loop(self) -> None:
        try:
            while True:
                await self.check_once()
                await asyncio.sleep(self.interval)
        except asyncio.CancelledError:
            return

    async def _probe(self, b: Backend) -> None:
        ok = False
        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(b.host, b.port), self.timeout)
            try:
                writer.write(
                    f"GET {self.path} HTTP/1.1\r\nHost: {b.address}\r\n"
                    f"Connection: close\r\n\r\n".encode())
                await writer.drain()
                resp = await asyncio.wait_for(read_response(reader), self.timeout)
                ok = 200 <= resp.status < 400
            finally:
                writer.close()
        except (OSError, asyncio.TimeoutError):
            ok = False
        except Exception:
            ok = False

        addr = b.address
        if ok:
            self._up[addr] = self._up.get(addr, 0) + 1
            self._down[addr] = 0
            if b.health == Health.DOWN and self._up[addr] >= self.rise:
                b.health = Health.UP
                b.consecutive_failures = 0
                b._breaker_until = 0.0
        else:
            self._down[addr] = self._down.get(addr, 0) + 1
            self._up[addr] = 0
            if b.health == Health.UP and self._down[addr] >= self.fall:
                b.health = Health.DOWN
