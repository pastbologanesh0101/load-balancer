# Load Balancer

A layer-7 HTTP **reverse proxy** and **load balancer** built from scratch in
pure Python (asyncio, no `aiohttp`, no `nginx`). It parses HTTP/1.1 itself,
spreads requests across a backend pool with a choice of algorithms, health-checks
the backends, retries failed requests, and trips a circuit breaker on backends
that keep erroring.

```
  clients ──HTTP/1.1──▶  ┌─────────── pylb ───────────┐
                         │ parse request              │
                         │ pick backend  (5 strategies)│──▶ origin :9001
                         │ rewrite headers (XFF, Via)  │──▶ origin :9002
                         │ forward, retry on failure   │──▶ origin :9003
                         │ circuit-break sick backends │
                         │ /_lb/stats  /_lb/health     │
                         └───────── health checker ────┘  (background GET /health)
```

## Quick start

```bash
git clone https://github.com/pastbologanesh0101/load-balancer
cd load-balancer
pip install -e ".[dev]"
pytest -q                                   # 31 tests (incl. end-to-end)
python examples/demo.py                      # every strategy + a failover run

# a real cluster you can curl:
pylb demo --backends 3 --strategy weighted --port 8080
for i in $(seq 9); do curl -s localhost:8080/ ; done
curl -s localhost:8080/_lb/stats
```

```
$ python examples/demo.py
round-robin  weights 1:3:1 -> {'o0': 20, 'o1': 20, 'o2': 20}  errors=0
weighted     weights 1:3:1 -> {'o0': 12, 'o1': 36, 'o2': 12}  errors=0
least-conn   weights 1:3:1 -> {'o0': 12, 'o1': 36, 'o2': 12}  errors=0
ip-hash      weights 1:3:1 -> {'o0': 60}  errors=0

failover: origin-1 killed -> {'o0': 15, 'o2': 15}  errors=0  retries=6
```

Point it at your own backends:

```bash
pylb serve --backend 10.0.0.1:8001 --backend 10.0.0.2:8001#3 \
           --strategy least-conn --port 8080 --health-path /healthz
```

## Concepts demonstrated

| Area | Where | What it shows |
|---|---|---|
| **HTTP/1.1** | `loadbalancer/http1.py` | Hand-written request/response reader: start line, a case-insensitive order-preserving header multimap, `obs-fold` continuations, bodies by `Content-Length` **and** `Transfer-Encoding: chunked`. Keep-alive and idempotency rules. |
| **Reverse proxy** | `loadbalancer/proxy.py` | Terminate the client connection, choose a backend, **rewrite headers** (`X-Forwarded-For` append, `X-Forwarded-Proto`, `Via`, strip hop-by-hop), forward, stream the response back, loop while keep-alive holds. |
| **Balancing algorithms** | `loadbalancer/balancers.py` | **round-robin**, **smooth weighted round-robin** (the nginx algorithm), **least-connections** (weight- and latency-aware), **ip-hash** (sticky per client), **random** — each unit-tested for its distribution. |
| **Active health checks** | `loadbalancer/health.py` | A background task probes `GET /health` on every backend; `rise`/`fall` streak thresholds flip `UP`/`DOWN` with hysteresis so a single blip doesn't eject a backend. |
| **Passive health / circuit breaking** | `loadbalancer/backends.py` | N consecutive failures open a per-backend breaker for a cooldown; the strategies skip unavailable backends automatically. |
| **Retries & failover** | `loadbalancer/proxy.py` | A failed **idempotent** request is retried on the next backend (up to `max_retries`); non-idempotent requests are not retried (at-most-once). |
| **Connection pooling** | `loadbalancer/proxy.py` | Idle keep-alive connections to each backend are pooled and reused; a stale pooled connection triggers one transparent re-dial. |
| **Observability** | `loadbalancer/proxy.py` | `GET /_lb/stats` → JSON with per-backend request/failure counts, active connections, EWMA latency, health; `GET /_lb/health` → 200/503. |

## How a request flows

```
read_request(client)
  └─ /_lb/*  ──▶ admin JSON
  └─ otherwise:
       rewrite headers  (XFF append, drop Connection/TE/Upgrade, set Content-Length)
       for attempt in 0..max_retries:            # only if the method is idempotent
           backend = strategy.pick(pool, {client_ip, path})   # skips tried + unavailable
           try:  forward over a pooled/new conn, read_response  (with timeout)
                 backend.record_success(latency);  add Via / X-LB-Backend;  return
           except OSError/Timeout/HttpError:
                 backend.record_failure()  →  maybe trip breaker;  continue
       └─ 502 / 503
```

## Limitations

- **HTTP/1.1 only** — no HTTP/2, no HTTPS termination (put it behind a TLS
  terminator), no `CONNECT` tunnelling, no WebSocket upgrade.
- **Bodies are fully buffered**, not streamed — large uploads/downloads sit in
  memory, and the request is re-sent with `Content-Length` (chunked is
  de-chunked).
- Health-check state and stats are in-process (no shared store across LB
  instances).
- `least-conn` counts connections this LB opened, not the backend's true load.

## Project layout

```
loadbalancer/
  http1.py      HTTP/1.1 reader/writer (headers multimap, chunked, keep-alive)
  backends.py   Backend + pool + circuit breaker
  balancers.py  round-robin / weighted / least-conn / ip-hash / random
  health.py     active health checker with rise/fall hysteresis
  proxy.py      LoadBalancer: accept, pick, rewrite, forward, retry, pool, stats
  origin.py     a tiny demo backend (/health, /slow, /kill, /revive)
  cli.py        pylb serve / pylb demo
examples/
  demo.py       strategy distributions + a failover run
tests/          http1 / balancers / backends / proxy (end-to-end with real origins)
```

## License

MIT
