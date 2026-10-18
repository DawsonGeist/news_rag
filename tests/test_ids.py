import pytest

from newsrag import ids


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("HTTPS://Example.COM/a/b", "https://example.com/a/b"),
        ("https://example.com", "https://example.com/"),
        ("https://example.com:443/x", "https://example.com/x"),
        ("http://example.com:80/x", "http://example.com/x"),
        ("http://example.com:8080/x", "http://example.com:8080/x"),
        ("https://user:pw@example.com/x", "https://example.com/x"),
        ("https://example.com/x#section", "https://example.com/x"),
        ("https://example.com/a/./b/../c", "https://example.com/a/c"),
        ("https://example.com/%7Euser/%2f", "https://example.com/~user/%2F"),
        ("https://example.com/café", "https://example.com/caf%C3%A9"),
        ("https://bücher.de/x", "https://xn--bcher-kva.de/x"),
        ("https://example.com/x?b=2&a=1", "https://example.com/x?a=1&b=2"),
        (
            "https://example.com/x?utm_source=tw&id=5&fbclid=abc&UTM_Medium=m",
            "https://example.com/x?id=5",
        ),
        ("https://example.com/x?utm_source=tw", "https://example.com/x"),
        ("  https://example.com/x  ", "https://example.com/x"),
    ],
)
def test_normalize_url(raw: str, expected: str) -> None:
    assert ids.normalize_url(raw) == expected


@pytest.mark.parametrize("bad", ["ftp://example.com/x", "mailto:a@b.c", "https:///x", "not a url"])
def test_normalize_url_rejects(bad: str) -> None:
    with pytest.raises(ids.InvalidURL):
        ids.normalize_url(bad)


def test_url_key_equivalences() -> None:
    variants = [
        "https://www.example.com/story/",
        "http://example.com/story",
        "https://EXAMPLE.com/story?utm_campaign=x#top",
        "http://www.example.com:80/story/",
    ]
    keys = {ids.url_key(v) for v in variants}
    assert keys == {"https://example.com/story"}


def test_url_key_keeps_distinct_content_params() -> None:
    assert ids.url_key("https://e.com/a?id=1") != ids.url_key("https://e.com/a?id=2")


def test_pinned_ids() -> None:
    # Pinned values: if these change, stored IDs change. That needs contracts/v2.
    url = "https://www.example.com/news/story-1?utm_source=x"
    assert ids.url_key(url) == "https://example.com/news/story-1"
    assert ids.doc_id(url) == ids.hash32("https://example.com/news/story-1")
    assert ids.hash32("https://example.com/news/story-1") == "598daa332b562040db3869ea0601d6cc"
    assert (
        ids.content_hash("Title", "Body.")
        == "4e41b7f7f851b617e6354d78f9e390d8312cda2090e9a927403992b1cfc5e77f"
    )


def test_id_shapes() -> None:
    assert len(ids.doc_id("https://e.com/a")) == 32
    assert ids.url_id("https://e.com/a") == ids.doc_id("https://e.com/a")
    assert len(ids.sha256_hex(b"x")) == 64


def test_registered_domain() -> None:
    assert ids.registered_domain("https://www.foxnews.com/politics/x") == "foxnews.com"
    assert ids.registered_domain("news.bbc.co.uk") == "bbc.co.uk"
    assert ids.registered_domain("abcnews.go.com") == "go.com"
    assert ids.registered_domain("127.0.0.1") == "127.0.0.1"


class TestChooseCanonical:
    def test_no_canonical_uses_final(self) -> None:
        assert ids.choose_canonical("https://e.com/a?utm_source=x", None) == "https://e.com/a"

    def test_same_site_canonical_wins(self) -> None:
        got = ids.choose_canonical("https://amp.e.com/a/amp", "https://www.e.com/a")
        assert got == "https://www.e.com/a"

    def test_root_canonical_ignored(self) -> None:
        assert ids.choose_canonical("https://e.com/a", "https://e.com/") == "https://e.com/a"

    def test_cross_site_canonical_ignored(self) -> None:
        got = ids.choose_canonical("https://local.com/a", "https://apnews.com/article/x")
        assert got == "https://local.com/a"

    def test_relative_or_bad_canonical_ignored(self) -> None:
        assert ids.choose_canonical("https://e.com/a", "/a") == "https://e.com/a"
        assert ids.choose_canonical("https://e.com/a", "javascript:x") == "https://e.com/a"
