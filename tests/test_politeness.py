import random
from datetime import UTC, datetime

import pytest

from newsrag.crawl import politeness as pol

NOW = datetime(2026, 10, 17, 12, 0, tzinfo=UTC)
RNG = random.Random(0)


def test_backoff_grows_4x_with_jitter() -> None:
    for attempt, base in [(1, 60), (2, 240), (3, 960)]:
        d = pol.backoff_delay_s(attempt, 60.0, RNG)
        assert 0.8 * base <= d <= 1.2 * base


@pytest.mark.parametrize(
    ("header", "expected"),
    [
        ("120", 120.0),
        ("Sat, 17 Oct 2026 12:05:00 GMT", 300.0),
        ("Sat, 17 Oct 2026 11:00:00 GMT", 0.0),
        ("soon", None),
        (None, None),
    ],
)
def test_parse_retry_after(header: str | None, expected: float | None) -> None:
    assert pol.parse_retry_after(header, NOW) == expected


@pytest.mark.parametrize(
    ("ctype", "ok"),
    [
        ("text/html; charset=utf-8", True),
        ("application/xhtml+xml", True),
        ("TEXT/HTML", True),
        (None, True),
        ("application/pdf", False),
        ("application/json", False),
    ],
)
def test_is_html(ctype: str | None, ok: bool) -> None:
    assert pol.is_html(ctype) is ok


def test_crawl_delay() -> None:
    assert pol.effective_crawl_delay(None, 5.0, 60.0) == 5.0
    assert pol.effective_crawl_delay(2.0, 5.0, 60.0) == 2.0
    assert pol.effective_crawl_delay(600.0, 5.0, 60.0) == 60.0


ROBOTS = (
    "User-agent: *\nDisallow: /all\n\nUser-agent: newsrag\nDisallow: /private\nCrawl-delay: 7\n"
)


def test_robots_verdicts() -> None:
    v = pol.robots_verdict
    assert v(200, ROBOTS, "https://e.com/private/x", "newsrag") == "disallow"
    assert v(200, ROBOTS, "https://e.com/all/x", "newsrag") == "allow"  # our group wins
    assert v(404, None, "https://e.com/private/x", "newsrag") == "allow"
    assert v(503, None, "https://e.com/x", "newsrag") == "unavailable"
    assert v(pol.ROBOTS_NETWORK_ERROR, None, "https://e.com/x", "newsrag") == "unavailable"
    assert pol.robots_crawl_delay(200, ROBOTS, "newsrag") == 7.0
    assert pol.robots_crawl_delay(404, None, "newsrag") is None


def decide(status: int, attempts: int, retry_after: float | None = None) -> pol.RetryDecision:
    return pol.decide_http_error(status, attempts, 5, retry_after, 60.0, 21600, RNG)


def test_http_error_rules() -> None:
    assert not decide(404, 1).retry and not decide(410, 1).retry
    assert decide(403, 1).retry and not decide(403, 2).retry  # other 4xx: one retry
    assert decide(500, 4).retry and not decide(500, 5).retry
    d = decide(429, 1, retry_after=600)
    assert d.retry and d.host_backoff_s == 600 and d.delay_s >= 600
    assert decide(503, 1, retry_after=10**9).host_backoff_s == 21600  # capped
    assert decide(503, 1).host_backoff_s is not None  # backs off the host even without header


def test_blocked_backoff_escalates_from_threshold() -> None:
    b = pol.blocked_backoff_s
    assert b(1, 2, 3600, 86400) is None  # a single 403 may be one forbidden page
    assert [b(n, 2, 3600, 86400) for n in (2, 3, 4)] == [3600.0, 7200.0, 14400.0]
    assert b(20, 2, 3600, 86400) == 86400.0  # capped at a day
