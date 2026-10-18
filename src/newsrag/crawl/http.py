"""Polite HTTP shared by the fetcher and the feed poller.

**One request in flight per host, across processes and components.** A claimant
(fetcher or poller) reserves a host inside its claim transaction by pushing
``hosts.next_allowed_at`` past the longest possible request (robots.txt plus the
resource, each bounded by the request timeout). No other claim can pick that host
until the request ends and ``release_host`` sets ``next_allowed_at = finished +
crawl_delay``. A crashed process's reservation simply lapses. The ``hosts`` table is
written only through this module.

Redirects are followed by hand so that robots.txt is checked on every hop.
"""

from __future__ import annotations

import logging
import sqlite3
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Literal
from urllib.parse import urljoin, urlsplit

import httpx

from newsrag import ids
from newsrag.config import ROBOTS_AGENT, Settings
from newsrag.crawl import politeness as pol
from newsrag.storage.state import transaction
from newsrag.timeutil import from_db, to_db, utcnow

log = logging.getLogger(__name__)

ROBOTS_MAX_BYTES = 500_000  # RFC 9309 asks crawlers to parse at least 500 KiB

GetKind = Literal[
    "ok",
    "not_modified",
    "http_error",
    "disallowed",
    "robots_unavailable",
    "network",
    "timeout",
    "bad_redirect",
    "too_many_redirects",
    "content_type",
    "too_large",
]


@dataclass
class GetResult:
    kind: GetKind
    final_url: str
    status: int | None = None
    content_type: str | None = None
    body: bytes | None = None
    etag: str | None = None
    last_modified: str | None = None
    retry_after_s: float | None = None
    error: str | None = None


def host_of(url: str) -> str:
    return (urlsplit(url).hostname or "").lower()


def reservation_s(settings: Settings) -> float:
    # robots.txt and the resource each get one timeout; slack covers commit time.
    return 2 * settings.fetch.request_timeout_s + settings.fetch.reservation_slack_s


HOST_READY_SQL = (
    "(h.next_allowed_at IS NULL OR h.next_allowed_at <= :now) "
    "AND (h.backoff_until IS NULL OR h.backoff_until <= :now)"
)
"""Condition on a ``hosts h`` join: the host is neither reserved nor backing off."""


def reserve_host(conn: sqlite3.Connection, settings: Settings, host: str, now: datetime) -> None:
    """Reserve ``host`` for one request. Call inside the claim transaction."""
    conn.execute(
        "INSERT INTO hosts (host, crawl_delay_s, next_allowed_at) VALUES (?, ?, ?) "
        "ON CONFLICT (host) DO UPDATE SET next_allowed_at = excluded.next_allowed_at",
        (
            host,
            settings.fetch.default_crawl_delay_s,
            to_db(now + timedelta(seconds=reservation_s(settings))),
        ),
    )


def release_host(
    conn: sqlite3.Connection,
    settings: Settings,
    host: str,
    now: datetime,
    *,
    network_error: bool = False,
    backoff_s: float | None = None,
) -> None:
    """End a reservation: the host is next allowed one crawl delay from now. Call inside
    the transaction that commits the request's result."""
    row = conn.execute("SELECT crawl_delay_s FROM hosts WHERE host = ?", (host,)).fetchone()
    delay = row["crawl_delay_s"] if row else settings.fetch.default_crawl_delay_s
    backoff = to_db(now + timedelta(seconds=backoff_s)) if backoff_s is not None else None
    conn.execute(
        "UPDATE hosts SET next_allowed_at = ?, "
        "backoff_until = coalesce(?, backoff_until), "
        "consecutive_errors = CASE WHEN ? THEN consecutive_errors + 1 ELSE 0 END "
        "WHERE host = ?",
        (to_db(now + timedelta(seconds=delay)), backoff, network_error, host),
    )


def make_client(settings: Settings) -> httpx.AsyncClient:
    """The one HTTP client configuration every request uses. Redirects are handled by
    ``PoliteClient.get`` so robots.txt is checked on every hop."""
    return httpx.AsyncClient(
        http2=True,
        follow_redirects=False,
        timeout=httpx.Timeout(settings.fetch.request_timeout_s),
        headers={"User-Agent": settings.user_agent, "From": settings.contact_email},
        limits=httpx.Limits(
            max_connections=settings.fetch.global_concurrency,
            max_keepalive_connections=min(32, settings.fetch.global_concurrency),
        ),
    )


def accept_any(content_type: str | None) -> bool:
    return True


class PoliteClient:
    def __init__(
        self, settings: Settings, conn: sqlite3.Connection, client: httpx.AsyncClient
    ) -> None:
        self.s = settings
        self.conn = conn
        self.client = client

    async def get(
        self,
        url: str,
        *,
        headers: Mapping[str, str] | None = None,
        accept: Callable[[str | None], bool] = pol.is_html,
    ) -> GetResult:
        """GET ``url`` with robots.txt checked on every hop. Never raises for HTTP or
        network problems; the caller classifies the result."""
        for _ in range(self.s.fetch.max_redirects + 1):
            verdict = await self.robots_check(url)
            if verdict == "disallow":
                return GetResult("disallowed", url)
            if verdict == "unavailable":
                return GetResult("robots_unavailable", url)
            try:
                result = await self._get_once(url, headers, accept)
            except httpx.TimeoutException:
                return GetResult("timeout", url, error="timeout")
            except httpx.TransportError as e:
                return GetResult("network", url, error=f"network:{type(e).__name__}")
            if isinstance(result, str):  # a redirect target
                try:
                    url = ids.normalize_url(result)
                except ids.InvalidURL:
                    return GetResult("bad_redirect", url)
                continue
            return result
        return GetResult("too_many_redirects", url)

    async def _get_once(
        self, url: str, headers: Mapping[str, str] | None, accept: Callable[[str | None], bool]
    ) -> GetResult | str:
        max_bytes = self.s.fetch.max_body_bytes
        async with self.client.stream("GET", url, headers=headers) as r:
            if r.status_code in pol.REDIRECT_STATUSES and "location" in r.headers:
                return urljoin(url, r.headers["location"])
            ctype = r.headers.get("content-type")
            res = GetResult(
                "ok",
                url,
                status=r.status_code,
                content_type=ctype,
                etag=r.headers.get("etag"),
                last_modified=r.headers.get("last-modified"),
            )
            if r.status_code == 304:
                res.kind = "not_modified"
                return res
            if not 200 <= r.status_code < 300:
                res.kind = "http_error"
                res.retry_after_s = pol.parse_retry_after(r.headers.get("retry-after"), utcnow())
                return res
            if not accept(ctype):
                res.kind = "content_type"
                return res
            declared = r.headers.get("content-length", "")
            if declared.isdigit() and int(declared) > max_bytes:
                res.kind = "too_large"
                return res
            body = bytearray()
            # Decoded bytes are counted, so a small gzip bomb is caught too.
            async for chunk in r.aiter_bytes():
                body += chunk
                if len(body) > max_bytes:
                    res.kind = "too_large"
                    return res
            res.body = bytes(body)
            return res

    async def robots_check(self, url: str) -> pol.RobotsVerdict:
        host = host_of(url)
        row = self.conn.execute(
            "SELECT robots_txt, robots_status, robots_fetched_at FROM hosts WHERE host = ?",
            (host,),
        ).fetchone()
        now = utcnow()
        if row is not None and row["robots_fetched_at"] is not None:
            definitive = row["robots_status"] is not None and 200 <= row["robots_status"] < 500
            # An unavailable robots.txt is re-asked at the next retry, not cached for a day.
            ttl = self.s.fetch.robots_ttl_s if definitive else self.s.fetch.retry_base_s
            if now - from_db(row["robots_fetched_at"]) < timedelta(seconds=ttl):
                return pol.robots_verdict(
                    row["robots_status"], row["robots_txt"], url, ROBOTS_AGENT
                )
        status, text = await self._fetch_robots(url)
        delay = pol.effective_crawl_delay(
            pol.robots_crawl_delay(status, text, ROBOTS_AGENT),
            self.s.fetch.default_crawl_delay_s,
            self.s.fetch.max_crawl_delay_s,
        )
        with transaction(self.conn):
            self.conn.execute(
                "INSERT INTO hosts (host, robots_txt, robots_status, robots_fetched_at, "
                "crawl_delay_s) VALUES (?, ?, ?, ?, ?) ON CONFLICT (host) DO UPDATE SET "
                "robots_txt = excluded.robots_txt, robots_status = excluded.robots_status, "
                "robots_fetched_at = excluded.robots_fetched_at, "
                "crawl_delay_s = excluded.crawl_delay_s",
                (host, text, status, to_db(now), delay),
            )
        return pol.robots_verdict(status, text, url, ROBOTS_AGENT)

    async def _fetch_robots(self, url: str) -> tuple[int, str | None]:
        parts = urlsplit(url)
        robots_url = f"{parts.scheme}://{parts.netloc}/robots.txt"
        try:
            r = await self.client.get(robots_url, follow_redirects=True)
        except httpx.HTTPError as e:
            log.info("robots.txt unreachable for %s: %s %s", parts.netloc, type(e).__name__, e)
            return pol.ROBOTS_NETWORK_ERROR, None
        text = r.content[:ROBOTS_MAX_BYTES].decode("utf-8", errors="replace")
        return r.status_code, text if 200 <= r.status_code < 300 else None
