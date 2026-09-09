"""Watch each strategy spread 60 requests across 3 origins, then kill one
backend and show failover. All async, all in one process.

    python examples/demo.py
"""

import asyncio
from collections import Counter

from loadbalancer.backends import Backend, BackendPool
from loadbalancer.http1 import read_response
from loadbalancer.origin import Origin
from loadbalancer.proxy import LoadBalancer


async def get(host, port, path="/"):
    r, w = await asyncio.open_connection(host, port)
    w.write(f"GET {path} HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n".encode())
    await w.drain()
    resp = await read_response(r)
    w.close()
    return resp


async def hammer(host, port, n):
    resps = await asyncio.gather(*(get(host, port) for _ in range(n)))
    return Counter(r.body.decode().split("served by ")[1].split(" ")[0]
                   for r in resps if r.status == 200), \
           sum(1 for r in resps if r.status != 200)


async def main():
    for strategy in ("round-robin", "weighted", "least-conn", "ip-hash"):
        origins = [Origin(f"o{i}") for i in range(3)]
        for o in origins:
            await o.start()
        pool = BackendPool([Backend(o.host, o.port, w)
                            for o, w in zip(origins, (1, 3, 1))])
        lb = LoadBalancer(pool, strategy, host="127.0.0.1", port=0, health_path=None)
        host, port = await lb.start()
        dist, errors = await hammer(host, port, 60)
        print(f"{strategy:<12} weights 1:3:1 -> {dict(sorted(dist.items()))}  errors={errors}")
        await lb.stop()
        for o in origins:
            await o.stop()

    # failover
    origins = [Origin(f"o{i}") for i in range(3)]
    for o in origins:
        await o.start()
    pool = BackendPool([Backend(o.host, o.port) for o in origins])
    lb = LoadBalancer(pool, "round-robin", host="127.0.0.1", port=0,
                      health_path=None, max_retries=2)
    host, port = await lb.start()
    await origins[1].stop()                       # backend 1 vanishes
    dist, errors = await hammer(host, port, 30)
    print(f"\nfailover: origin-1 killed -> {dict(sorted(dist.items()))}  "
          f"errors={errors}  retries={lb.stats['retries']}")
    await lb.stop()
    for o in origins:
        await o.stop()


if __name__ == "__main__":
    asyncio.run(main())
