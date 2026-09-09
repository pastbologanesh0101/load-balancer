"""End-to-end: real origin servers behind the real proxy, driven by a real
HTTP client."""

import asyncio
import contextlib
import json
from collections import Counter

from loadbalancer.backends import Backend, BackendPool
from loadbalancer.http1 import read_response
from loadbalancer.origin import Origin
from loadbalancer.proxy import LoadBalancer


def run(coro):
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        pending = asyncio.all_tasks(loop)
        for t in pending:
            t.cancel()
        with contextlib.suppress(Exception):
            loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
        loop.close()


async def http_get(host, port, path="/", headers=None, body=b""):
    reader, writer = await asyncio.open_connection(host, port)
    hlines = "".join(f"{k}: {v}\r\n" for k, v in (headers or {}).items())
    writer.write(f"GET {path} HTTP/1.1\r\nHost: t\r\n{hlines}"
                 f"Content-Length: {len(body)}\r\nConnection: close\r\n\r\n".encode()
                 + body)
    await writer.drain()
    resp = await read_response(reader)
    writer.close()
    return resp


async def spin_up(n=3, **lb_kw):
    origins = [Origin(f"o{i}") for i in range(n)]
    for o in origins:
        await o.start()
    pool = BackendPool([Backend(o.host, o.port) for o in origins])
    lb = LoadBalancer(pool, host="127.0.0.1", port=0, health_path=None, **lb_kw)
    host, port = await lb.start()
    return origins, pool, lb, host, port


async def tear_down(origins, lb):
    await lb.stop()
    for o in origins:
        await o.stop()


def test_round_robin_spreads_load():
    async def scenario():
        origins, pool, lb, host, port = await spin_up(3, strategy="round-robin")
        try:
            servers = []
            for _ in range(9):
                r = await http_get(host, port, "/")
                servers.append(r.body.decode().split("served by ")[1].split(" ")[0])
            assert Counter(servers) == Counter({"o0": 3, "o1": 3, "o2": 3})
        finally:
            await tear_down(origins, lb)
    run(scenario())


def test_forwarded_headers_and_via_added():
    async def scenario():
        origins, pool, lb, host, port = await spin_up(1)
        try:
            # make the origin echo back what it received
            origins[0]._route = _echo_route.__get__(origins[0])
            r = await http_get(host, port, "/echo",
                               headers={"X-Forwarded-For": "203.0.113.9"})
            echoed = json.loads(r.body.decode())
            assert echoed["headers"]["x-forwarded-for"].endswith("127.0.0.1")
            assert echoed["headers"]["x-forwarded-for"].startswith("203.0.113.9")
            assert echoed["headers"]["x-forwarded-proto"] == "http"
            # the client sent "Connection: close"; the LB replaces it with its
            # own value for the upstream link rather than propagating it
            assert echoed["headers"]["connection"] == "keep-alive"
            assert r.headers.get("via") == "1.1 pylb"
            assert r.headers.get("x-lb-backend") == f"{origins[0].host}:{origins[0].port}"
        finally:
            await tear_down(origins, lb)
    run(scenario())


def test_failover_retries_next_backend():
    async def scenario():
        origins, pool, lb, host, port = await spin_up(3, strategy="round-robin",
                                                      max_retries=2)
        try:
            await origins[1].stop()          # kill one backend outright
            ok = 0
            for _ in range(12):
                r = await http_get(host, port, "/")
                if r.status == 200:
                    ok += 1
            assert ok == 12                  # every request served by a live backend
            assert lb.stats["retries"] >= 1
            assert pool.backends[1].total_failures >= 1
        finally:
            await tear_down(origins, lb)
    run(scenario())


def test_all_backends_down_returns_503():
    async def scenario():
        origins, pool, lb, host, port = await spin_up(2)
        try:
            for o in origins:
                await o.stop()
            r = await http_get(host, port, "/")
            assert r.status in (502, 503)
            assert lb.stats["failed"] + lb.stats["no_backend"] >= 1
        finally:
            await tear_down(origins, lb)
    run(scenario())


def test_non_idempotent_not_retried():
    async def scenario():
        origins, pool, lb, host, port = await spin_up(2, strategy="round-robin")
        try:
            await origins[0].stop()
            # POST to the dead backend first -> should NOT retry -> 502
            reader, writer = await asyncio.open_connection(host, port)
            writer.write(b"POST / HTTP/1.1\r\nHost: t\r\nContent-Length: 2\r\n"
                         b"Connection: close\r\n\r\nhi")
            await writer.drain()
            r = await read_response(reader)
            writer.close()
            assert r.status == 502
            assert lb.stats["retries"] == 0
        finally:
            await tear_down(origins, lb)
    run(scenario())


def test_admin_stats_endpoint():
    async def scenario():
        origins, pool, lb, host, port = await spin_up(2)
        try:
            for _ in range(4):
                await http_get(host, port, "/")
            r = await http_get(host, port, "/_lb/stats")
            data = json.loads(r.body.decode())
            assert data["counters"]["requests"] >= 5
            assert len(data["backends"]) == 2
            assert sum(b["total_requests"] for b in data["backends"]) >= 4
        finally:
            await tear_down(origins, lb)
    run(scenario())


def test_health_check_ejects_and_readmits():
    async def scenario():
        origins = [Origin(f"o{i}") for i in range(3)]
        for o in origins:
            await o.start()
        pool = BackendPool([Backend(o.host, o.port) for o in origins])
        lb = LoadBalancer(pool, host="127.0.0.1", port=0,
                          health_path="/health", health_interval=0.05)
        host, port = await lb.start()
        try:
            await lb._health.check_once()
            origins[1].healthy = False               # start failing /health
            for _ in range(4):
                await lb._health.check_once()
            assert pool.backends[1].health.value == "down"

            servers = set()
            for _ in range(12):
                r = await http_get(host, port, "/")
                servers.add(r.body.decode().split("served by ")[1].split(" ")[0])
            assert "o1" not in servers               # not routed to the down backend

            origins[1].healthy = True
            for _ in range(4):
                await lb._health.check_once()
            assert pool.backends[1].health.value == "up"
        finally:
            await lb.stop()
            for o in origins:
                await o.stop()
    run(scenario())


def test_least_conn_balances_under_concurrency():
    async def scenario():
        origins, pool, lb, host, port = await spin_up(3, strategy="least-conn")
        try:
            # fire 30 concurrent slow requests; least-conn should spread them evenly
            async def one():
                return await http_get(host, port, "/slow",
                                      headers={"x-delay": "0.15"})
            results = await asyncio.gather(*(one() for _ in range(30)))
            assert all(r.status == 200 for r in results)
            served = Counter(r.body.decode().split("served by ")[1].split(" ")[0]
                             for r in results)
            assert max(served.values()) - min(served.values()) <= 4
        finally:
            await tear_down(origins, lb)
    run(scenario())


# helper injected into an Origin to echo the request it received
async def _echo_route(self, req):
    from loadbalancer.http1 import Response
    self.count += 1
    payload = json.dumps({
        "method": req.method,
        "target": req.target,
        "headers": {k.lower(): v for k, v in req.headers.items()},
        "body": req.body.decode("latin-1"),
    })
    return Response.simple(200, "OK", payload, content_type="application/json")
