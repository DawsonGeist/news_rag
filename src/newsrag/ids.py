"""Identifier and hashing rules (contracts/v1/README.md, section "Identifiers").

Everything in this module is part of the v1 contract. Changing any output for an
existing input changes IDs of stored data, which is a breaking change and needs
contracts/v2. ``tests/test_ids.py`` pins known values to catch accidents.
"""

from __future__ import annotations

import hashlib
import re
from functools import lru_cache
from urllib.parse import parse_qsl, quote, urlencode, urlsplit, urlunsplit

CANONICALIZER_VERSION = "url-v1"

# Query parameters that never identify content. Matched case-insensitively.
# Deliberately conservative: wrongly stripping a content parameter merges two
# articles into one doc_id, while wrongly keeping a tracking parameter only
# produces a duplicate that content_hash catches later.
TRACKING_PARAMS = frozenset(
    {
        "fbclid",
        "gclid",
        "dclid",
        "gbraid",
        "wbraid",
        "msclkid",
        "yclid",
        "twclid",
        "igshid",
        "mc_cid",
        "mc_eid",
        "ocid",
        "cmpid",
        "cmp",
        "smid",
        "smtyp",
        "sr_share",
        "ref",
        "ref_src",
        "ref_url",
        "referrer",
        "taid",
        "ito",
        "ncid",
        "ns_mchannel",
        "ns_source",
        "ns_campaign",
        "ns_linkname",
        "ns_fee",
        "spref",
        "soc_src",
        "soc_trk",
        "dicbo",
        "tpcc",
        "guccounter",
        "guce_referrer",
        "guce_referrer_sig",
        "_ga",
        "_gl",
        "_hsenc",
        "_hsmi",
        "__twitter_impression",
        "s_cid",
        "intcmp",
        "int_cmp",
    }
)
TRACKING_PREFIXES = ("utm_", "pk_", "hsa_", "oly_", "vero_", "mkt_", "at_")

_UNRESERVED = frozenset("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-._~")
_PCT = re.compile(r"%([0-9A-Fa-f]{2})")
_PATH_SAFE = "/%:@!$&'()*+,;="


class InvalidURL(ValueError):
    pass


def _normalize_escapes(s: str) -> str:
    """Decode percent-escapes of unreserved characters, uppercase the rest (RFC 3986 6.2.2)."""

    def repl(m: re.Match[str]) -> str:
        ch = chr(int(m.group(1), 16))
        return ch if ch in _UNRESERVED else "%" + m.group(1).upper()

    return _PCT.sub(repl, s)


def _remove_dot_segments(path: str) -> str:
    """RFC 3986 section 5.2.4."""
    out: list[str] = []
    for seg in path.split("/"):
        if seg == "..":
            if len(out) > 1:
                out.pop()
        elif seg != ".":
            out.append(seg)
    result = "/".join(out)
    if path.endswith(("/.", "/..")):
        result += "/"
    return result if result.startswith("/") else "/" + result


def _normalize_path(path: str) -> str:
    if not path:
        return "/"
    path = quote(path, safe=_PATH_SAFE)
    path = _normalize_escapes(path)
    return _remove_dot_segments(path)


def _is_tracking(key: str) -> bool:
    k = key.lower()
    return k in TRACKING_PARAMS or k.startswith(TRACKING_PREFIXES)


def _normalize_query(query: str) -> str:
    if not query:
        return ""
    pairs = [(k, v) for k, v in parse_qsl(query, keep_blank_values=True) if not _is_tracking(k)]
    pairs.sort()
    return urlencode(pairs, quote_via=quote)


def normalize_url(url: str) -> str:
    """Return the canonical form of a URL. The result is still a real, fetchable URL.

    Lowercases scheme and host, IDNA-encodes the host, drops userinfo, default
    ports and the fragment, normalizes percent-escapes and dot segments, removes
    tracking parameters and sorts the remaining query parameters.
    """
    parts = urlsplit(url.strip())
    scheme = parts.scheme.lower()
    if scheme not in ("http", "https"):
        raise InvalidURL(f"unsupported scheme: {url!r}")
    host = (parts.hostname or "").rstrip(".")
    if not host:
        raise InvalidURL(f"missing host: {url!r}")
    try:
        host = host.encode("idna").decode("ascii").lower()
    except UnicodeError as e:
        raise InvalidURL(f"bad host: {url!r}") from e
    try:
        port = parts.port
    except ValueError as e:
        raise InvalidURL(f"bad port: {url!r}") from e
    if ":" in host:  # IPv6 literal
        host = f"[{host}]"
    default_port = {"http": 80, "https": 443}[scheme]
    netloc = host if port in (None, default_port) else f"{host}:{port}"
    return urlunsplit(
        (scheme, netloc, _normalize_path(parts.path), _normalize_query(parts.query), "")
    )


def url_key(url: str) -> str:
    """Identity key of a URL: normalized, scheme forced to https, leading ``www.``
    removed, trailing slash removed (except for the root path).

    The key is what gets hashed into IDs; it is not necessarily fetchable.
    """
    parts = urlsplit(normalize_url(url))
    host = parts.netloc.removeprefix("www.")
    path = parts.path
    if len(path) > 1:
        path = path.rstrip("/") or "/"
    return urlunsplit(("https", host, path, parts.query, ""))


def hash32(text: str) -> str:
    """First 32 hex characters of sha256 over UTF-8 text."""
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:32]


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def url_id(url: str) -> str:
    """Frontier ID of a discovered URL (before fetching)."""
    return hash32(url_key(url))


def doc_id(canonical_url: str) -> str:
    """Document ID: hash of the canonical URL's identity key."""
    return hash32(url_key(canonical_url))


def feed_id(feed_url: str) -> str:
    return hash32(url_key(feed_url))


def content_hash(title: str | None, extracted_text: str) -> str:
    """Revision identity of a Document: title plus body. A headline change is a new revision."""
    return sha256_hex(((title or "") + "\n\n" + extracted_text).encode("utf-8"))


@lru_cache(maxsize=1)
def _tld_extractor():  # type: ignore[no-untyped-def]
    import tldextract

    # Bundled public-suffix snapshot only: no network, deterministic per tldextract version.
    return tldextract.TLDExtract(suffix_list_urls=(), cache_dir=None)


def registered_domain(url_or_host: str) -> str:
    """eTLD+1 of a URL or host, e.g. ``https://www.foxnews.com/x`` -> ``foxnews.com``.

    Falls back to the bare host for IPs and single-label hosts.
    """
    host = urlsplit(url_or_host).hostname if "//" in url_or_host else url_or_host
    host = (host or "").lower().rstrip(".")
    ext = _tld_extractor()(host)
    if hasattr(ext, "top_domain_under_public_suffix"):  # tldextract >= 5.3
        rd = ext.top_domain_under_public_suffix
    else:
        rd = ext.registered_domain
    return rd or host


def choose_canonical(final_url: str, rel_canonical: str | None) -> str:
    """Pick the canonical URL of a fetched page (normalized).

    ``rel=canonical`` wins only when it is an absolute http(s) URL on the same
    registered domain as the final URL and does not point at the site root
    (some sites set every page's canonical to the homepage). Otherwise the
    final URL after redirects is used.
    """
    final = normalize_url(final_url)
    if not rel_canonical:
        return final
    try:
        cand = normalize_url(rel_canonical)
    except InvalidURL:
        return final
    if urlsplit(cand).path in ("", "/"):
        return final
    if registered_domain(cand) != registered_domain(final):
        return final
    return cand
