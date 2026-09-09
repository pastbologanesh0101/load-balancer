"""Selection strategies. ``pick(pool, ctx)`` returns a healthy backend or None.

``ctx`` carries per-request info a strategy might need (e.g. the client IP for
consistent hashing).
"""

from __future__ import annotations

import hashlib
import itertools
import random
from dataclasses import dataclass

from .backends import Backend, BackendPool


@dataclass
class Context:
    client_ip: str = "0.0.0.0"
    path: str = "/"


class Strategy:
    name = "base"

    def pick(self, pool: BackendPool, ctx: Context) -> Backend | None:
        raise NotImplementedError


class RoundRobin(Strategy):
    name = "round-robin"

    def __init__(self):
        self._i = 0

    def pick(self, pool, ctx):
        avail = pool.available()
        if not avail:
            return None
        b = avail[self._i % len(avail)]
        self._i += 1
        return b


class WeightedRoundRobin(Strategy):
    """Smooth weighted round-robin (the nginx algorithm)."""

    name = "weighted"

    def __init__(self):
        self._current: dict[str, int] = {}

    def pick(self, pool, ctx):
        avail = pool.available()
        if not avail:
            return None
        total = 0
        best = None
        for b in avail:
            cur = self._current.get(b.address, 0) + b.weight
            self._current[b.address] = cur
            total += b.weight
            if best is None or cur > self._current[best.address]:
                best = b
        self._current[best.address] -= total
        return best


class LeastConnections(Strategy):
    name = "least-conn"

    def pick(self, pool, ctx):
        avail = pool.available()
        if not avail:
            return None
        # tie-break by weight (prefer higher) then latency (prefer lower)
        return min(avail, key=lambda b: (b.active_conns / b.weight, b.ewma_latency_ms))


class IpHash(Strategy):
    """Consistent-ish: same client IP -> same backend while the set is stable."""

    name = "ip-hash"

    def pick(self, pool, ctx):
        avail = pool.available()
        if not avail:
            return None
        h = int(hashlib.md5(ctx.client_ip.encode()).hexdigest(), 16)
        return avail[h % len(avail)]


class Random(Strategy):
    name = "random"

    def __init__(self, seed: int | None = None):
        self._rng = random.Random(seed)

    def pick(self, pool, ctx):
        avail = pool.available()
        return self._rng.choice(avail) if avail else None


REGISTRY = {c.name: c for c in
            (RoundRobin, WeightedRoundRobin, LeastConnections, IpHash, Random)}


def make_strategy(name: str) -> Strategy:
    try:
        return REGISTRY[name]()
    except KeyError:
        raise ValueError(f"unknown strategy {name!r}; choose from {sorted(REGISTRY)}")
