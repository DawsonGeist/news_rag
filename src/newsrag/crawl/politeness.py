"""Pure retry and politeness rules for the fetcher (docs/phase1-design.md, section 3).

No I/O here, so every rule is unit-testable without a network or a database.
"""

from __future__ import annotations

import random
from dataclasses import dataclass
from datetime import datetime
from email.utils import parsedate_to_datetime
from typing import Literal

from protego import Protego

from newsrag.timeutil import ensure_utc

HTML_TYPES = frozenset({"text/html", "application/xhtml+xml"})
REDIRECT_STATUSES = frozenset({301, 302, 303, 307, 308})

# robots_status value stored when robots.txt could not be fetched at all (DNS, TLS,
# timeout). HTTP statuses are stored as-is; NULL means never fetched.
ROBOTS_NETWORK_ERROR = 0

RobotsVerdict = Literal["allow", "disallow", "unavailable"]


def backoff_delay_s(attempts: int, base_s: float, rng: random.Random) -> float:
    """Delay before retry number ``attempts`` (1-based): base * 4^(n-1), +-20% jitter."""
    return base_s * float(4 ** max(0, attempts - 1)) * rng.uniform(0.8, 1.2)


def parse_retry_after(value: str | None, now: datetime) -> float | None:
    """Seconds from ``now`` named by a Retry-After header (delta-seconds or HTTP-date)."""
    if not value:
        return None
    value = value.strip()
    if value.isdigit():
        return float(value)
    try:
        when = parsedate_to_datetime(value)
    except (TypeError, ValueError):
        return None
    if when.tzinfo is None:  # HTTP-dates are GMT by definition
        return None
    return max(0.0, (ensure_utc(when) - now).total_seconds())


def is_html(content_type: str | None) -> bool:
    """Whether to keep a response. A missing Content-Type is kept: the extractor decides
    what it is, and dropping it would lose pages from misconfigured servers."""
    if not content_type:
        return True
    return content_type.split(";", 1)[0].strip().lower() in HTML_TYPES


def effective_crawl_delay(robots_delay: float | None, default_s: float, max_s: float) -> float:
    """robots.txt Crawl-delay if given, else the default; never above ``max_s``."""
    delay = default_s if robots_delay is None else robots_delay
    return max(0.0, min(delay, max_s))


def robots_verdict(
    status: int | None, robots_txt: str | None, url: str, agent: str
) -> RobotsVerdict:
    """RFC 9309: 4xx means no restrictions; 5xx or unreachable means assume full disallow
    for now (the URL is retried later, not skipped)."""
    if status is None or status == ROBOTS_NETWORK_ERROR or status >= 500:
        return "unavailable"
    if status >= 400:
        return "allow"
    if 200 <= status < 300:
        rp = Protego.parse(robots_txt or "")
        return "allow" if rp.can_fetch(url, agent) else "disallow"
    return "unavailable"  # 3xx left over after following redirects, 1xx


def robots_crawl_delay(status: int | None, robots_txt: str | None, agent: str) -> float | None:
    if status is None or not 200 <= status < 300:
        return None
    delay = Protego.parse(robots_txt or "").crawl_delay(agent)
    return float(delay) if delay is not None else None


@dataclass(frozen=True)
class RetryDecision:
    retry: bool  # False: give up (state 'failed')
    delay_s: float  # before the URL may be fetched again (when retry)
    host_backoff_s: float | None  # push back every URL on the host
    error_code: str


def decide_http_error(
    status: int,
    attempts: int,
    max_attempts: int,
    retry_after_s: float | None,
    base_s: float,
    max_retry_after_s: float,
    rng: random.Random,
) -> RetryDecision:
    """What to do after a non-2xx, non-redirect response. ``attempts`` includes this one."""
    code = f"http_{status}"
    backoff = backoff_delay_s(attempts, base_s, rng)
    if status in (404, 410):
        return RetryDecision(False, 0.0, None, code)
    if status in (429, 503):
        # The server told us to slow down: honour it for the whole host.
        wait = min(retry_after_s, max_retry_after_s) if retry_after_s is not None else backoff
        return RetryDecision(attempts < max_attempts, max(wait, backoff), wait, code)
    if 400 <= status < 500:
        return RetryDecision(attempts < min(2, max_attempts), backoff, None, code)
    return RetryDecision(attempts < max_attempts, backoff, None, code)


def blocked_backoff_s(streak: int, threshold: int, start_s: float, max_s: float) -> float | None:
    """Host back-off after ``streak`` consecutive failures that include a 403.

    One 403 can be a single forbidden page, so nothing happens below ``threshold``.
    From there the pause doubles per further block, capped at ``max_s``: a host that
    has blocked us is asked again rarely, never URL by URL at the crawl delay.
    """
    if streak < threshold:
        return None
    return float(min(max_s, start_s * 2 ** (streak - threshold)))


def decide_transient(
    error_code: str, attempts: int, max_attempts: int, base_s: float, rng: random.Random
) -> RetryDecision:
    """Network errors, timeouts and unavailable robots.txt: retry with backoff."""
    return RetryDecision(
        attempts < max_attempts, backoff_delay_s(attempts, base_s, rng), None, error_code
    )
