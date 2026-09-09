from collections import Counter

import pytest

from loadbalancer.backends import Backend, BackendPool, Health
from loadbalancer.balancers import (
    Context,
    IpHash,
    LeastConnections,
    RoundRobin,
    WeightedRoundRobin,
    make_strategy,
)


def pool(*weights):
    return BackendPool([Backend("127.0.0.1", 9000 + i, w) for i, w in enumerate(weights)])


CTX = Context(client_ip="10.0.0.5")


def test_round_robin_cycles():
    p = pool(1, 1, 1)
    s = RoundRobin()
    picks = [s.pick(p, CTX).port for _ in range(6)]
    assert picks == [9000, 9001, 9002, 9000, 9001, 9002]


def test_round_robin_skips_unavailable():
    p = pool(1, 1, 1)
    p.backends[1].health = Health.DOWN
    s = RoundRobin()
    ports = {s.pick(p, CTX).port for _ in range(10)}
    assert ports == {9000, 9002}


def test_weighted_distribution_matches_weights():
    p = pool(1, 3, 1)          # total 5
    s = WeightedRoundRobin()
    counts = Counter(s.pick(p, CTX).port for _ in range(5000))
    # backend 9001 should get ~3/5 of traffic
    assert 0.55 < counts[9001] / 5000 < 0.65
    assert abs(counts[9000] - counts[9002]) < 200
    # smooth WRR: never more than `weight` in a row is a soft property; check no starvation
    assert counts[9000] > 800 and counts[9002] > 800


def test_least_connections_prefers_idle_backend():
    p = pool(1, 1, 1)
    p.backends[0].active_conns = 5
    p.backends[1].active_conns = 2
    p.backends[2].active_conns = 8
    s = LeastConnections()
    assert s.pick(p, CTX).port == 9001


def test_least_connections_normalises_by_weight():
    p = pool(1, 4)
    p.backends[0].active_conns = 2     # 2/1 = 2.0
    p.backends[1].active_conns = 6     # 6/4 = 1.5  -> preferred
    assert LeastConnections().pick(p, CTX).port == 9001


def test_ip_hash_is_stable_per_client():
    p = pool(1, 1, 1, 1)
    s = IpHash()
    a = {s.pick(p, Context(client_ip="1.2.3.4")).port for _ in range(20)}
    b = {s.pick(p, Context(client_ip="9.9.9.9")).port for _ in range(20)}
    assert len(a) == 1 and len(b) == 1     # each client pinned to one backend


def test_no_available_backend_returns_none():
    p = pool(1, 1)
    for b in p.backends:
        b.health = Health.DOWN
    for name in ("round-robin", "weighted", "least-conn", "ip-hash", "random"):
        assert make_strategy(name).pick(p, CTX) is None


def test_unknown_strategy_raises():
    with pytest.raises(ValueError):
        make_strategy("magic")
