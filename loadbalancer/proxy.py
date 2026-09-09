"""The reverse proxy.

Per client connection: read a full HTTP/1.1 request, choose a backend with the
configured strategy, forward the (rewritten) request, stream the response back,
and repeat while keep-alive holds. A failed *idempotent* request is retried on
the next backend; repeated failures trip a per-backend circuit breaker.
"""

from __future__ import annotations

import asyncio
import json
import time

from .backends import BackendPool
from .balancers import Context, Strategy, make_strategy
from .health import HealthChecker
from .http1 import (
    Headers,
    HttpError,
    Request,
    Response,
    read_request,
    read_response,
)

HOP_BY_HOP = {"connection", "keep-alive", "proxy-authenticate", "proxy-authorization",
              "te", "trailers", "transfer-encoding", "upgrade"}


class LoadBalancer:
    def __init__(self, pool: BackendPool, strategy: Strategy | str = "round-robin", *,
                 host: str = "127.0.0.1", port: int = 8080,
                 max_retries: int = 2, request_timeout: float = 10.0,
                 breaker_threshold: int = 3, breaker_cooldown: float = 5.0,
                 health_path: str | None = "/health", health_interval: float = 1.0,
                 pool_size: int = 8):
        self.pool = pool
        self.strategy = make_strategy(strategy) if isinstance(strategy, str) else strategy
        self.host, self.port = host, port
        self.max_retries = max_retries
        self.request_timeout = request_timeout
        self.breaker_threshold = breaker_threshold
        self.breaker_cooldown = breaker_cooldown
        self.pool_size = pool_size

        self._server: asyncio.AbstractServer | None = None
        self._health = (HealthChecker(pool, health_path, interval=health_interval)
                        if health_path else None)
        self._idle: dict[str, list[tuple]] = {}
        self.started_at = 0.0
        self.stats = {"requests": 0, "responses": 0, "retries": 0,
                      "failed": 0, "no_backend": 0}

    # -- lifecycle -----------------------------------------
    async def start(self) -> tuple[str, int]:
        self._server = await asyncio.start_server(self._handle_client, self.host, self.port)
        self.host, self.port = self._server.sockets[0].getsockname()[:2]
        self.started_at = time.time()
        if self._health:
            self._health.start()
        return self.host, self.port

    async def stop(self) -> None:
        if self._health:
            await self._health.stop()
        if self._server:
            self._server.close()
            await self._server.wait_closed()
        for conns in self._idle.values():
            for _r, w in conns:
                w.close()
        self._idle.clear()

    async def serve_forever(self) -> None:
        await self.start()
        async with self._server:
            await self._server.serve_forever()

    # -- client side --------------------------------------
    async def _handle_client(self, reader, writer) -> None:
        peer = writer.get_extra_info("peername")
        client_ip = peer[0] if peer else "0.0.0.0"
        try:
            while True:
                try:
                    req = await read_request(reader)
                except (asyncio.IncompleteReadError, ConnectionResetError):
                    return
                except HttpError as e:
                    writer.write(Response.simple(e.status, e.reason).render())
                    await writer.drain()
                    return

                self.stats["requests"] += 1
                if req.target.startswith("/_lb/"):
                    resp = self._admin(req)
                else:
                    resp = await self._proxy(req, client_ip)

                keep = req.keep_alive and resp.headers.get("connection", "").lower() != "close"
                resp.headers.set("Connection", "keep-alive" if keep else "close")
                writer.write(resp.render())
                await writer.drain()
                self.stats["responses"] += 1
                if not keep:
                    return
        except (ConnectionResetError, BrokenPipeError):
            pass
        finally:
            writer.close()

    # -- proxying ----------------------------------------
    async def _proxy(self, req: Request, client_ip: str) -> Response:
        ctx = Context(client_ip=client_ip, path=req.target)
        fwd = self._rewrite_request(req, client_ip)
        tried: set[str] = set()

        attempts = self.max_retries + 1 if req.idempotent else 1
        for attempt in range(attempts):
            backend = self._pick_excluding(ctx, tried)
            if backend is None:
                self.stats["no_backend"] += 1
                return Response.simple(503, "Service Unavailable",
                                       "no healthy backend\n")
            tried.add(backend.address)
            backend.active_conns += 1
            t0 = time.monotonic()
            try:
                resp = await asyncio.wait_for(self._forward(backend, fwd, req.method),
                                              self.request_timeout)
                backend.record_success((time.monotonic() - t0) * 1000)
                resp.headers.add("Via", "1.1 pylb")
                resp.headers.set("X-LB-Backend", backend.address)
                return resp
            except (OSError, asyncio.TimeoutError, asyncio.IncompleteReadError,
                    HttpError) as e:
                backend.record_failure(breaker_threshold=self.breaker_threshold,
                                       breaker_cooldown=self.breaker_cooldown)
                if attempt + 1 < attempts:
                    self.stats["retries"] += 1
                    continue
                self.stats["failed"] += 1
                return Response.simple(502, "Bad Gateway",
                                       f"upstream error: {type(e).__name__}\n")
            finally:
                backend.active_conns -= 1
        # unreachable
        return Response.simple(502, "Bad Gateway")

    def _pick_excluding(self, ctx, tried):
        # temporarily hide already-tried backends from the strategy
        original = self.pool.backends
        candidates = [b for b in original if b.address not in tried]
        if not candidates:
            return None
        self.pool.backends = candidates
        try:
            return self.strategy.pick(self.pool, ctx)
        finally:
            self.pool.backends = original

    def _rewrite_request(self, req: Request, client_ip: str) -> Request:
        h = Headers()
        for k, v in req.headers.items():
            if k.lower() in HOP_BY_HOP:
                continue
            h.add(k, v)
        xff = req.headers.get("x-forwarded-for")
        h.set("X-Forwarded-For", f"{xff}, {client_ip}" if xff else client_ip)
        h.set("X-Forwarded-Proto", "http")
        h.set("Content-Length", str(len(req.body)))
        h.set("Connection", "keep-alive")
        return Request(req.method, req.target, "HTTP/1.1", h, req.body)

    async def _forward(self, backend, req: Request, method: str) -> Response:
        reader, writer, reused = await self._dial(backend)
        try:
            writer.write(req.render())
            await writer.drain()
            resp = await read_response(reader, head=(method == "HEAD"))
        except Exception:
            writer.close()
            if reused:                          # stale pooled conn: retry once fresh
                reader, writer, _ = await self._dial(backend, force_new=True)
                writer.write(req.render())
                await writer.drain()
                resp = await read_response(reader, head=(method == "HEAD"))
            else:
                raise
        # return the connection to the pool if both sides want to keep it
        if resp.headers.get("connection", "").lower() != "close" \
                and resp.headers.get("content-length") is not None \
                and not reader.at_eof():
            self._release(backend.address, reader, writer)
        else:
            writer.close()
        return resp

    async def _dial(self, backend, force_new: bool = False):
        if not force_new:
            bucket = self._idle.get(backend.address, [])
            while bucket:
                reader, writer = bucket.pop()
                if not reader.at_eof() and not writer.is_closing():
                    return reader, writer, True
                writer.close()
        reader, writer = await asyncio.open_connection(backend.host, backend.port)
        return reader, writer, False

    def _release(self, address: str, reader, writer) -> None:
        bucket = self._idle.setdefault(address, [])
        if len(bucket) < self.pool_size:
            bucket.append((reader, writer))
        else:
            writer.close()

    # -- admin ---------------------------------------
    def _admin(self, req: Request) -> Response:
        if req.target.rstrip("/") == "/_lb/stats":
            body = json.dumps({
                "uptime_s": round(time.time() - self.started_at, 1),
                "strategy": self.strategy.name,
                "counters": self.stats,
                "backends": [b.snapshot() for b in self.pool.backends],
            }, indent=2)
            return Response.simple(200, "OK", body + "\n", content_type="application/json")
        if req.target.rstrip("/") == "/_lb/health":
            healthy = len(self.pool.available())
            status = 200 if healthy else 503
            return Response.simple(status, "OK" if healthy else "Service Unavailable",
                                   f"{healthy}/{len(self.pool.backends)} backends up\n")
        return Response.simple(404, "Not Found", "unknown admin endpoint\n")
