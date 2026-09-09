"""``pylb`` command line.

    pylb serve --backend 127.0.0.1:9001 --backend 127.0.0.1:9002#3 \\
               --strategy weighted --port 8080
    pylb demo   --backends 3           # spin up origins + balancer, print URLs
"""

from __future__ import annotations

import argparse
import asyncio
import signal

from .backends import BackendPool
from .balancers import REGISTRY
from .origin import run_origins
from .proxy import LoadBalancer


async def _serve(ns) -> None:
    pool = BackendPool.parse(ns.backend)
    lb = LoadBalancer(pool, ns.strategy, host=ns.host, port=ns.port,
                      max_retries=ns.retries, request_timeout=ns.timeout,
                      health_path=None if ns.no_health else ns.health_path)
    host, port = await lb.start()
    print(f"pylb listening on http://{host}:{port}  strategy={ns.strategy}")
    print(f"  backends: {[b.address for b in pool.backends]}")
    print(f"  stats:    http://{host}:{port}/_lb/stats")
    await _wait_for_signal()
    await lb.stop()


async def _demo(ns) -> None:
    origins = await run_origins(ns.backends)
    pool = BackendPool([__import__("loadbalancer").Backend(o.host, o.port)
                        for o in origins])
    lb = LoadBalancer(pool, ns.strategy, host="127.0.0.1", port=ns.port,
                      health_interval=0.5)
    host, port = await lb.start()
    print(f"demo up: {ns.backends} origins behind http://{host}:{port} "
          f"(strategy={ns.strategy})")
    print("try:  for i in $(seq 8); do curl -s http://%s:%d/ ; done" % (host, port))
    print(f"      curl -s http://{host}:{port}/_lb/stats")
    print("Ctrl-C to stop.")
    try:
        await _wait_for_signal()
    finally:
        await lb.stop()
        for o in origins:
            await o.stop()


async def _wait_for_signal() -> None:
    stop = asyncio.Event()
    loop = asyncio.get_event_loop()
    for s in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(s, stop.set)
    await stop.wait()


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="pylb")
    sub = p.add_subparsers(dest="cmd", required=True)

    s = sub.add_parser("serve")
    s.add_argument("--backend", action="append", required=True,
                   help="host:port or host:port#weight (repeatable)")
    s.add_argument("--strategy", default="round-robin", choices=list(REGISTRY))
    s.add_argument("--host", default="127.0.0.1")
    s.add_argument("--port", type=int, default=8080)
    s.add_argument("--retries", type=int, default=2)
    s.add_argument("--timeout", type=float, default=10.0)
    s.add_argument("--health-path", default="/health")
    s.add_argument("--no-health", action="store_true")
    s.set_defaults(func=_serve)

    s = sub.add_parser("demo")
    s.add_argument("--backends", type=int, default=3)
    s.add_argument("--strategy", default="round-robin", choices=list(REGISTRY))
    s.add_argument("--port", type=int, default=8080)
    s.set_defaults(func=_demo)
    return p


def main(argv=None) -> int:
    ns = build_parser().parse_args(argv)
    asyncio.run(ns.func(ns))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
