import asyncio

import pytest

from loadbalancer.http1 import (
    Headers,
    Request,
    Response,
    read_request,
    read_response,
)


async def _parse(fn, data: bytes):
    r = asyncio.StreamReader()          # inside a running loop -> no deprecation
    r.feed_data(data)
    r.feed_eof()
    return await fn(r)


def parse_request(data: bytes) -> Request:
    return asyncio.run(_parse(read_request, data))


def parse_response(data: bytes) -> Response:
    return asyncio.run(_parse(read_response, data))


def test_headers_case_insensitive_multivalue():
    h = Headers()
    h.add("X-Foo", "a")
    h.add("x-foo", "b")
    assert h.get("X-FOO") == "a"
    assert h.get_all("x-foo") == ["a", "b"]
    h.set("X-Foo", "c")
    assert h.get_all("x-foo") == ["c"]


def test_parse_request_with_content_length():
    req = parse_request((
        b"POST /submit HTTP/1.1\r\nHost: x\r\nContent-Length: 5\r\n\r\nhello"))
    assert (req.method, req.target, req.version) == ("POST", "/submit", "HTTP/1.1")
    assert req.headers.get("host") == "x"
    assert req.body == b"hello"


def test_parse_chunked_body():
    resp = parse_response((
        b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n"
        b"5\r\nhello\r\n6\r\n world\r\n0\r\n\r\n"))
    assert resp.status == 200
    assert resp.body == b"hello world"


def test_obs_fold_header_continuation():
    req = parse_request((
        b"GET / HTTP/1.1\r\nX-Long: part1\r\n  part2\r\n\r\n"))
    assert req.headers.get("x-long") == "part1 part2"


@pytest.mark.parametrize("version,conn,expected", [
    ("HTTP/1.1", None, True),
    ("HTTP/1.1", "close", False),
    ("HTTP/1.0", None, False),
    ("HTTP/1.0", "keep-alive", True),
])
def test_keep_alive_detection(version, conn, expected):
    h = Headers()
    if conn:
        h.set("Connection", conn)
    assert Request("GET", "/", version, h).keep_alive is expected


def test_response_render_roundtrip():
    resp = Response.simple(404, "Not Found", "nope\n")
    parsed = parse_response((resp.render()))
    assert parsed.status == 404 and parsed.body == b"nope\n"


def test_idempotent_flag():
    mk = lambda m: Request(m, "/", "HTTP/1.1", Headers())
    assert mk("GET").idempotent and mk("PUT").idempotent
    assert not mk("POST").idempotent and not mk("PATCH").idempotent
