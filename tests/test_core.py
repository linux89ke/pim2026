import pandas as pd

from learned_rules import normalize_learned_flag
from processing_automation import similarity_label, cluster_image_keys
<<<<<<< HEAD
=======
from general_rules import build_validators
>>>>>>> 66238a0 (Prevent maintenance lubricants from FDA sexual wellness flag)


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
<<<<<<< HEAD
=======


def test_maintenance_lubricant_is_not_sexual_wellness_fda_match():
    """A non-sexual lubricant category must not trigger the FDA rule."""
    path = (
        "Health & Beauty / Beauty & Personal Care / Hair Care / Hair Cutting Tools / "
        "Hair Clippers & Accessories / Accessories / Lubricants & Cleaners"
    )
    frame = pd.DataFrame([{
        "PRODUCT_SET_SID": "27f4aea9-c266-483e-b991-96f32188ca6f",
        "NAME": "Multi-Function Window and Door Lubricant Spray",
        "BRAND": "Generic",
        "CATEGORY": "Lubricants & Cleaners",
        "FULL_CATEGORY_PATH": path,
        "CATEGORY_CODE": "1001762",
        "FDA": "",
    }])
    validators = build_validators({"code_to_path": {"1001762": path}}, "UG")
    fda = next(
        (fn for flag, fn, _kwargs in validators
         if flag == "Sexual wellness product without FDA registration"),
        None,
    )
    assert fda is not None
    assert fda(frame).empty
>>>>>>> 66238a0 (Prevent maintenance lubricants from FDA sexual wellness flag)

