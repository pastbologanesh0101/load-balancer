"""A tiny demo origin server so you can watch the balancer spread load.

Every response says which instance answered and how many requests it has seen.
``/health`` returns 200 (or 503 after ``fail_after`` requests, to demo ejection).
``/slow`` sleeps. ``/kill`` makes it start failing.
"""

from __future__ import annotations

import asyncio
import time

from .http1 import Response, read_request


class Origin:
    def __init__(self, name: str, host: str = "127.0.0.1", port: int = 0):
        self.name = name
        self.host, self.port = host, port
        self.count = 0
        self.healthy = True
        self._server: asyncio.AbstractServer | None = None

    async def start(self) -> tuple[str, int]:
        self._server = await asyncio.start_server(self._handle, self.host, self.port)
        self.host, self.port = self._server.sockets[0].getsockname()[:2]
        return self.host, self.port

    async def stop(self) -> None:
        if self._server:
            self._server.close()
            await self._server.wait_closed()

    async def _handle(self, reader, writer) -> None:
        try:
            while True:
                try:
                    req = await read_request(reader)
                except (asyncio.IncompleteReadError, ConnectionResetError):
                    return
                resp = await self._route(req)
                keep = req.keep_alive
                resp.headers.set("Connection", "keep-alive" if keep else "close")
                writer.write(resp.render())
                await writer.drain()
                if not keep:
                    return
        except (ConnectionResetError, BrokenPipeError):
            pass
        finally:
            writer.close()

    async def _route(self, req) -> Response:
        self.count += 1
        path = req.target.split("?")[0]
        if path == "/health":
            return (Response.simple(200, "OK", "ok\n") if self.healthy
                    else Response.simple(503, "Service Unavailable", "unhealthy\n"))
        if path == "/kill":
            self.healthy = False
            return Response.simple(200, "OK", f"{self.name} now failing health\n")
        if path == "/revive":
            self.healthy = True
            return Response.simple(200, "OK", f"{self.name} healthy again\n")
        if path == "/slow":
            await asyncio.sleep(float(req.headers.get("x-delay", "0.5")))
        if not self.healthy and path == "/":
            return Response.simple(500, "Internal Server Error", f"{self.name} is down\n")
        return Response.simple(
            200, "OK",
            f"served by {self.name} (request #{self.count}) path={path}\n")


async def run_origins(n: int, base_port: int = 0) -> list[Origin]:
    origins = [Origin(f"origin-{i}", port=(base_port + i if base_port else 0))
               for i in range(n)]
    for o in origins:
        await o.start()
    return origins
