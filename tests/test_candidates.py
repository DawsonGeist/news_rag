from newsrag.contracts.v1 import FeedMedium, FeedOrigin
from newsrag.feeds.candidates import extract_candidates

WORLD = {
    "Canada": {"cbc.ca": {"languages": ["en"], "feeds": {"Local News": ["https://cbc.ca/rss"]}}},
    "United States": {
        "www.example.com": {
            "rank": 10,
            "languages": ["en"],
            "feeds": {
                "Politics & Society": ["https://example.com/politics.xml"],
                "Opinion": ["https://example.com/politics.xml?utm_source=list"],  # duplicate
                "Podcasts: Sports": ["https://pods.example.net/sports.rss"],
                "Other": ["https://example.com/misc.xml"],
            },
        },
        "elpais-us.com": {
            "languages": ["es"],
            "feeds": {"Local News": ["https://elpais-us.com/rss"]},
        },
        "mixed.com": {"languages": ["es", "en"], "feeds": {"Sports": ["https://mixed.com/rss"]}},
    },
}
SUPPLEMENT = {
    "United States": {
        "cnn.com": {
            "name": "CNN",
            "languages": ["en"],
            "feeds": {"Politics & Society": ["http://rss.cnn.com/rss/cnn_allpolitics.rss"]},
        },
        "www.example.com": {"feeds": {"Politics & Society": ["https://example.com/politics.xml"]}},
    }
}


def test_extract_candidates() -> None:
    cands, stats = extract_candidates(
        [(WORLD, FeedOrigin.FEEDS_YAML), (SUPPLEMENT, FeedOrigin.SUPPLEMENT)]
    )
    by_url = {c.feed_url: c for c in cands}

    assert set(by_url) == {
        "https://example.com/politics.xml",
        "https://pods.example.net/sports.rss",
        "https://mixed.com/rss",
        "http://rss.cnn.com/rss/cnn_allpolitics.rss",
    }
    politics = by_url["https://example.com/politics.xml"]
    assert politics.categories == ["Politics & Society", "Opinion"]
    assert politics.source_id == "example.com"
    assert politics.listed_domain == "www.example.com"
    assert politics.origin is FeedOrigin.FEEDS_YAML  # first listing wins

    pod = by_url["https://pods.example.net/sports.rss"]
    assert pod.medium is FeedMedium.AUDIO
    assert pod.categories == ["Sports"]
    assert pod.source_id == "example.com"  # publisher from the list, not the feed host

    cnn = by_url["http://rss.cnn.com/rss/cnn_allpolitics.rss"]
    assert cnn.origin is FeedOrigin.SUPPLEMENT
    assert cnn.display_name == "CNN"

    assert stats.domains_non_english == 1
    assert stats.feeds_excluded_category == 1
    assert stats.feeds_merged_duplicate == 2
