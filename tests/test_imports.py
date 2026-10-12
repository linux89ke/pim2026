"""Smoke-test imports used by local and Streamlit Cloud deployments."""

import importlib


def test_core_modules_import_without_optional_ui_startup():
    for module_name in (
        "data_utils",
        "learned_rules",
        "processing_automation",
        "category_matcher_engine",
    ):
        importlib.import_module(module_name)
