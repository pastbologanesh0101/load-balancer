"""The backend pool: one :class:`Backend` per upstream, health state, and
passive circuit-breaking.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from enum import Enum


class Health(str, Enum):
    UP = "up"
    DOWN = "down"


@dataclass
class Backend:
    host: str
    port: int
    weight: int = 1
    # runtime state
    health: Health = Health.UP
    active_conns: int = 0
    total_requests: int = 0
    total_failures: int = 0
    consecutive_failures: int = 0
    ewma_latency_ms: float = 0.0
    _breaker_until: float = 0.0

    def __post_init__(self):
        if self.weight < 1:
            raise ValueError("weight must be >= 1")

    @property
    def address(self) -> str:
        return f"{self.host}:{self.port}"

    # -- circuit breaker ------------------------------------
    def available(self, now: float | None = None) -> bool:
        now = time.monotonic() if now is None else now
        return self.health == Health.UP and now >= self._breaker_until

    def record_success(self, latency_ms: float) -> None:
        self.total_requests += 1
        self.consecutive_failures = 0
        # exponentially weighted moving average
        a = 0.2
        self.ewma_latency_ms = (a * latency_ms + (1 - a) * self.ewma_latency_ms
                                if self.ewma_latency_ms else latency_ms)

    def record_failure(self, *, breaker_threshold: int = 3,
                       breaker_cooldown: float = 5.0,
                       now: float | None = None) -> None:
        now = time.monotonic() if now is None else now
        self.total_requests += 1
        self.total_failures += 1
        self.consecutive_failures += 1
        if self.consecutive_failures >= breaker_threshold:
            self._breaker_until = now + breaker_cooldown

    def snapshot(self) -> dict:
        return {
            "address": self.address,
            "weight": self.weight,
            "health": self.health.value,
            "available": self.available(),
            "active_conns": self.active_conns,
            "total_requests": self.total_requests,
            "total_failures": self.total_failures,
            "consecutive_failures": self.consecutive_failures,
            "ewma_latency_ms": round(self.ewma_latency_ms, 2),
        }


class BackendPool:
    def __init__(self, backends: list[Backend]):
        if not backends:
            raise ValueError("need at least one backend")
        self.backends = backends

    def available(self, now: float | None = None) -> list[Backend]:
        return [b for b in self.backends if b.available(now)]

    def by_address(self, address: str) -> Backend | None:
        for b in self.backends:
            if b.address == address:
                return b
        return None

    @classmethod
    def parse(cls, specs: list[str]) -> "BackendPool":
        """Each spec is ``host:port`` or ``host:port#weight``."""
        out = []
        for spec in specs:
            addr, _, w = spec.partition("#")
            host, port = addr.rsplit(":", 1)
            out.append(Backend(host, int(port), int(w) if w else 1))
        return cls(out)
