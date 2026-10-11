import pandas as pd

from learned_rules import normalize_learned_flag
from processing_automation import similarity_label, cluster_image_keys


def test_learned_flag_aliases_are_canonical():
    assert normalize_learned_flag("Poor image") == "Poor images"
    assert normalize_learned_flag("suspected fake") == "Suspected Fake product"


def test_similarity_labels_are_explicit():
    assert similarity_label("url") == "Exact URL match"
    assert similarity_label("phash") == "Exact pHash match"
    assert similarity_label("near-phash", 5) == "Near-pHash match: distance 5/64"


def test_image_clusters_group_by_phash_or_url():
    rows = [
        {"product_set_sid": "a", "phash": "abc"},
        {"product_set_sid": "b", "phash": "abc"},
        {"product_set_sid": "c", "image_url": "https://example.test/c.jpg"},
    ]
    clusters = cluster_image_keys(rows)
    assert len(clusters["abc"]) == 2
    assert len(clusters["https://example.test/c.jpg"]) == 1
