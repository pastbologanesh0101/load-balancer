"""A small HTTP/1.1 reader/writer over asyncio streams.

Enough of the spec for a reverse proxy: the request/status line, header folding
into a case-insensitive multimap, and bodies delimited by ``Content-Length`` or
``Transfer-Encoding: chunked``. ``read_message`` works for both requests and
responses.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field

MAX_HEADER_BYTES = 64 * 1024


class HttpError(Exception):
    def __init__(self, status: int, reason: str):
        super().__init__(f"{status} {reason}")
        self.status = status
        self.reason = reason


class Headers:
    """Case-insensitive, order-preserving, multi-value header container."""

    def __init__(self, items: list[tuple[str, str]] | None = None):
        self._items: list[tuple[str, str]] = list(items or [])

    def get(self, name: str, default=None):
        low = name.lower()
        for k, v in self._items:
            if k.lower() == low:
                return v
        return default

    def get_all(self, name: str) -> list[str]:
        low = name.lower()
        return [v for k, v in self._items if k.lower() == low]

    def set(self, name: str, value: str) -> None:
        self.remove(name)
        self._items.append((name, value))

    def add(self, name: str, value: str) -> None:
        self._items.append((name, value))

    def remove(self, name: str) -> None:
        low = name.lower()
        self._items = [(k, v) for k, v in self._items if k.lower() != low]

    def __contains__(self, name: str) -> bool:
        return self.get(name) is not None

    def items(self):
        return list(self._items)

    def render(self) -> bytes:
        return b"".join(f"{k}: {v}\r\n".encode("latin-1") for k, v in self._items)


@dataclass
class Request:
    method: str
    target: str
    version: str
    headers: Headers
    body: bytes = b""

    @property
    def keep_alive(self) -> bool:
        conn = (self.headers.get("connection") or "").lower()
        if self.version == "HTTP/1.0":
            return conn == "keep-alive"
        return conn != "close"

    @property
    def idempotent(self) -> bool:
        return self.method in ("GET", "HEAD", "PUT", "DELETE", "OPTIONS", "TRACE")

    def render(self) -> bytes:
        return (f"{self.method} {self.target} {self.version}\r\n".encode("latin-1")
                + self.headers.render() + b"\r\n" + self.body)


@dataclass
class Response:
    version: str
    status: int
    reason: str
    headers: Headers = field(default_factory=Headers)
    body: bytes = b""

    def render(self) -> bytes:
        return (f"{self.version} {self.status} {self.reason}\r\n".encode("latin-1")
                + self.headers.render() + b"\r\n" + self.body)

    @classmethod
    def simple(cls, status: int, reason: str, body: str = "", *,
               content_type: str = "text/plain") -> "Response":
        b = body.encode()
        h = Headers()
        h.set("Content-Type", content_type)
        h.set("Content-Length", str(len(b)))
        h.set("Connection", "close")
        return cls("HTTP/1.1", status, reason, h, b)


async def _read_headers(reader: asyncio.StreamReader) -> tuple[bytes, Headers]:
    raw = await reader.readuntil(b"\r\n\r\n")
    if len(raw) > MAX_HEADER_BYTES:
        raise HttpError(431, "Request Header Fields Too Large")
    head, _, _ = raw.partition(b"\r\n\r\n")
    lines = head.split(b"\r\n")
    start_line = lines[0].decode("latin-1")
    headers = Headers()
    for line in lines[1:]:
        if not line:
            continue
        if line[:1] in (b" ", b"\t"):        # obs-fold: append to previous
            if headers._items:
                k, v = headers._items[-1]
                headers._items[-1] = (k, v + " " + line.strip().decode("latin-1"))
            continue
        name, _, value = line.partition(b":")
        headers.add(name.decode("latin-1").strip(), value.decode("latin-1").strip())
    return start_line.encode(), headers


async def _read_body(reader: asyncio.StreamReader, headers: Headers) -> bytes:
    te = (headers.get("transfer-encoding") or "").lower()
    if "chunked" in te:
        chunks = bytearray()
        while True:
            size_line = (await reader.readuntil(b"\r\n")).strip()
            size = int(size_line.split(b";")[0], 16)
            if size == 0:
                await reader.readuntil(b"\r\n")   # trailing CRLF (ignore trailers)
                break
            chunks += await reader.readexactly(size)
            await reader.readexactly(2)           # CRLF after each chunk
        return bytes(chunks)
    cl = headers.get("content-length")
    if cl is not None:
        return await reader.readexactly(int(cl))
    return b""


async def read_request(reader: asyncio.StreamReader) -> Request:
    start, headers = await _read_headers(reader)
    try:
        method, target, version = start.decode("latin-1").split(" ", 2)
    except ValueError:
        raise HttpError(400, "Bad Request")
    body = await _read_body(reader, headers)
    return Request(method, target, version, headers, body)


async def read_response(reader: asyncio.StreamReader, *, head: bool = False) -> Response:
    start, headers = await _read_headers(reader)
    parts = start.decode("latin-1").split(" ", 2)
    version, status = parts[0], int(parts[1])
    reason = parts[2] if len(parts) > 2 else ""
    body = b"" if head or status in (204, 304) else await _read_body(reader, headers)
    return Response(version, status, reason, headers, body)
