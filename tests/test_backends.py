import pytest

from loadbalancer.backends import Backend, BackendPool


def test_weight_must_be_positive():
    with pytest.raises(ValueError):
        Backend("h", 1, weight=0)


def test_circuit_breaker_trips_then_recovers():
    b = Backend("h", 1)
    now = 100.0
    assert b.available(now)
    for _ in range(2):
        b.record_failure(breaker_threshold=3, breaker_cooldown=5.0, now=now)
    assert b.available(now)                       # not tripped yet
    b.record_failure(breaker_threshold=3, breaker_cooldown=5.0, now=now)
    assert not b.available(now)                   # tripped
    assert not b.available(now + 4.9)
    assert b.available(now + 5.1)                 # cooldown elapsed


def test_success_resets_consecutive_failures():
    b = Backend("h", 1)
    b.record_failure(now=0.0)
    b.record_failure(now=0.0)
    b.record_success(12.0)
    assert b.consecutive_failures == 0
    assert b.ewma_latency_ms == 12.0
    b.record_success(20.0)
    assert 12 < b.ewma_latency_ms < 20            # EWMA, not a jump


def test_pool_parse_with_weights():
    p = BackendPool.parse(["10.0.0.1:80", "10.0.0.2:8080#5"])
    assert (p.backends[0].address, p.backends[0].weight) == ("10.0.0.1:80", 1)
    assert (p.backends[1].address, p.backends[1].weight) == ("10.0.0.2:8080", 5)


def test_empty_pool_rejected():
    with pytest.raises(ValueError):
        BackendPool([])
