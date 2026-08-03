"""
constants.py - Shared constants used across all modules
"""

# Kept as a dict with the original keys because ~26 call sites across
# streamlit_app, ui_components and export_utils index it directly. The values
# now resolve from design_tokens so there is one palette rather than four —
# see the module docstring there for why the orange had to be split into a
# fill colour and a (darker) text colour.
from design_tokens import JUMIA_COLORS_OVERRIDE as _TOKENS

JUMIA_COLORS = {
    'primary_orange': '#F68B1E',
    'secondary_orange': '#FF9933',
    'jumia_red': '#E73C17',
    'dark_gray': '#313133',
    'medium_gray': '#5A5A5C',
    'light_gray': '#F5F5F5',
    'border_gray': '#E0E0E0',
    'success_green': '#4CAF50',
    'warning_yellow': '#FFC107',
    'white': '#FFFFFF',
    'black': '#000000'
}
JUMIA_COLORS.update(_TOKENS)

PRODUCTSETS_COLS = ["ProductSetSid", "ParentSKU", "Status", "ReasonOne", "Comment", "FLAG", "SellerName"]
REJECTION_REASONS_COLS = ['CODE - REJECTION_REASON', 'COMMENT']

FULL_DATA_COLS = [
    "PRODUCT_SET_SID", "ACTIVE_STATUS_COUNTRY", "NAME", "BRAND", "CATEGORY", "CATEGORY_CODE",
    "FULL_CATEGORY_PATH",
    "COLOR", "COLOR_FAMILY", "MAIN_IMAGE", "VARIATION", "PARENTSKU", "SELLER_NAME", "SELLER_SKU",
    "GLOBAL_PRICE", "GLOBAL_SALE_PRICE", "TAX_CLASS", "FLAG", "LISTING_STATUS",
    "PRODUCT_WARRANTY", "WARRANTY_DURATION", "WARRANTY_ADDRESS", "WARRANTY_TYPE", "COUNT_VARIATIONS",
    "LIST_VARIATIONS"
]

GRID_COLS = ['PRODUCT_SET_SID', 'NAME', 'BRAND', 'CATEGORY', 'SELLER_NAME', 'MAIN_IMAGE', 'GLOBAL_SALE_PRICE', 'GLOBAL_PRICE', 'COLOR']

COUNTRY_CURRENCY = {
    "Kenya":       {"code": "KES", "symbol": "KSh", "pair": "USD/KES"},
    "Uganda":      {"code": "UGX", "symbol": "USh", "pair": "USD/UGX"},
    "Nigeria":     {"code": "NGN", "symbol": "₦",   "pair": "USD/NGN"},
    "Ghana":       {"code": "GHS", "symbol": "GH₵", "pair": "USD/GHS"},
    "Morocco":     {"code": "MAD", "symbol": "MAD", "pair": "USD/MAD"},
    "Egypt":       {"code": "EGP", "symbol": "EGP", "pair": "USD/EGP"},
    "Senegal":     {"code": "XOF", "symbol": "XOF", "pair": "USD/XOF"},
    "Ivory Coast": {"code": "XOF", "symbol": "XOF", "pair": "USD/XOF"},
}

NEW_FILE_MAPPING = {
    'cod_productset_sid': 'PRODUCT_SET_SID',
    "2qz3wx4ec5rv6b7hnj8kl;'[]": 'PRODUCT_SET_SID',
    'dsc_name': 'NAME',
    'dsc_brand_name': 'BRAND',
    'cod_category_code': 'CATEGORY_CODE',
    'dsc_category_name': 'CATEGORY',
    'dsc_shop_seller_name': 'SELLER_NAME',
    'dsc_shop_active_country': 'ACTIVE_STATUS_COUNTRY',
    'cod_parent_sku': 'PARENTSKU',
    'parentsku': 'PARENTSKU',
    'parent_sku': 'PARENTSKU',
    'parent sku': 'PARENTSKU',
    'color': 'COLOR',
    'colour': 'COLOR',
    'color_family': 'COLOR_FAMILY',
    'colour_family': 'COLOR_FAMILY',
    'colour family': 'COLOR_FAMILY',
    'color family': 'COLOR_FAMILY',
    'COLOUR FAMILY': 'COLOR_FAMILY',
    'list_seller_skus': 'SELLER_SKU',
    'image1': 'MAIN_IMAGE',
    'image_1': 'MAIN_IMAGE',
    'main_image': 'MAIN_IMAGE',
    'main image': 'MAIN_IMAGE',
    'image': 'MAIN_IMAGE',
    'img': 'MAIN_IMAGE',
    'img_url': 'MAIN_IMAGE',
    'image_url': 'MAIN_IMAGE',
    'photo': 'MAIN_IMAGE',
    'dsc_status': 'LISTING_STATUS',
    'dsc_shop_email': 'SELLER_EMAIL',
    'product_warranty': 'PRODUCT_WARRANTY',
    'warranty_duration': 'WARRANTY_DURATION',
    'warranty_address': 'WARRANTY_ADDRESS',
    'warranty_type': 'WARRANTY_TYPE',
    'count_variations': 'COUNT_VARIATIONS',
    'count variations': 'COUNT_VARIATIONS',
    'number of variations': 'COUNT_VARIATIONS',
    'list_variations': 'LIST_VARIATIONS',
    'list variations': 'LIST_VARIATIONS'
}

COLOR_PATTERNS = {
    'red': ['red', 'crimson', 'scarlet', 'maroon', 'burgundy', 'wine', 'ruby'],
    'blue': ['blue', 'navy', 'royal', 'sky', 'azure', 'cobalt', 'sapphire'],
    'green': ['green', 'lime', 'olive', 'emerald', 'mint', 'forest', 'jade'],
    'black': ['black', 'onyx', 'ebony', 'jet', 'charcoal', 'midnight'],
    'white': ['white', 'ivory', 'cream', 'pearl', 'snow', 'alabaster'],
    'gray': ['gray', 'grey', 'silver', 'slate', 'ash', 'graphite'],
    'yellow': ['yellow', 'gold', 'golden', 'amber', 'lemon', 'mustard'],
    'orange': ['orange', 'tangerine', 'peach', 'coral', 'apricot'],
    'pink': ['pink', 'rose', 'magenta', 'fuchsia', 'salmon', 'blush'],
    'purple': ['purple', 'violet', 'lavender', 'plum', 'mauve', 'lilac'],
    'brown': ['brown', 'tan', 'beige', 'khaki', 'chocolate', 'coffee', 'bronze'],
    'multicolor': ['multicolor', 'multicolour', 'multi-color', 'rainbow', 'mixed']
}

COUNTRY_VALIDATOR_CONFIG = {
    "Kenya": {"code": "KE", "skip_validations": []},
    "Uganda": {
        "code": "UG",
        "skip_validations": [],
    },
    "Nigeria":     {"code": "NG", "skip_validations": []},
    "Ghana":       {"code": "GH", "skip_validations": []},
    "Morocco":     {"code": "MA", "skip_validations": ["Generic BRAND Issues"]},
    "Egypt":       {"code": "EG", "skip_validations": []},
    "Senegal":     {"code": "SN", "skip_validations": []},
    "Ivory Coast": {"code": "CI", "skip_validations": []},
}


# Pre-computed map for faster extraction in data_utils
COLOR_VARIANT_TO_BASE = {}
for _base_color, _variants in COLOR_PATTERNS.items():
    for _variant in _variants:
        COLOR_VARIANT_TO_BASE[_variant] = _base_color

REASON_MAP = {
    "REJECT_POOR_IMAGE":     "Poor images",
    "REJECT_IMG_STRETCHED":  "Image Stretched",
    "REJECT_IMG_BLURRY":     "Image Blurry",
    "REJECT_IMG_MISMATCH":   "Image Mismatch",
    "REJECT_IMG_INFRINGING": "Image Infringing",
    "REJECT_IMG_TOO_MANY":   "Image Too Many things displayed",
    "REJECT_WRONG_CAT":      "Wrong Category",
    "REJECT_FAKE":           "Suspected Fake product",
    "REJECT_BRAND":          "Restricted brands",
    "REJECT_PROHIBITED":     "Prohibited products",
    "REJECT_NSFW":           "Adult / NSFW content",
    "REJECT_COLOR":          "Missing COLOR",
    "REJECT_WRONG_BRAND":    "Generic branded products with genuine brands",
    "REJECT_SUSP_DISCOUNT":  "Discount too high",
    "REJECT_DUPLICATE":      "Duplicate product",
    "REJECT_WEIGHT_VOL":     "Missing Weight/Volume",
    "REJECT_BRAND_REPEAT":   "BRAND name repeated in NAME",
    "REJECT_BRAND_IN_NAME":  "BRAND name repeated in NAME",
    "REJECT_WARRANTY":       "Product Warranty",
    "REJECT_VARIATION":      "Wrong Variation",
    "REJECT_FDA":            "FDA",
    "REJECT_TITLE_LANG":     "Title Language Check",
    "REJECT_FAKE_PERFUME":   "Suspected Fake Perfume",
    "REJECT_SUSPICIOUS_DISCOUNT": "Suspicious Discount",
    "REJECT_REFURB":         "Seller Not approved to sell Refurb",
    "REJECT_BOOKS_SELLER":   "Seller Approve to sell books",
    "REJECT_PERFUME_SELLER": "Seller Approved to Sell Perfume",
    "REJECT_PERFUME_TESTER": "Perfume Tester",
    "REJECT_SNEAKERS":       "Counterfeit Sneakers",
    "REJECT_JERSEYS":        "Suspected counterfeit Jerseys",
    "REJECT_UNNECESSARY_WORDS": "Unnecessary words in NAME",
    "REJECT_SINGLE_WORD":    "Single-word NAME",
    "REJECT_GENERIC_BRAND":  "Generic BRAND Issues",
    "REJECT_FASHION_BRAND":  "Fashion brand issues",
    "REJECT_SMARTPHONE_NAME": "Incomplete Smartphone Name",
    "REJECT_BRAND_MISMATCH": "Brand Image Mismatch",
    "REJECT_OFFPLATFORM":    "Off-Platform Contact",
    "REJECT_SPECS_INCONSISTENCY": "Specs Inconsistency",
    "OTHER_CUSTOM":          "Other Reason (Custom)"
}

SPLIT_LIMIT = 9998
MULTI_COUNTRY_VALUES = {'MULTIPLE', 'MULTI'}
PARQUET_CACHE_DIR = "app_cache_parquet"
FLAG_CACHE_DIR = "app_cache_flags"

# ── Image aspect ratio tiers ───────────────────────────────────────────────
# ratio = height / width. Lives here because two places need it and neither
# can own it: check_image_stretched in streamlit_app.py decides what is
# rejected, and the grid in ui_components.py decides what is merely flagged
# for a person to look at. ui_components cannot import from streamlit_app —
# streamlit_app imports it — so the shared module is the only honest home.
#
# One threshold used to do both jobs, at 1.5 / 0.6. The grid drew a
# "Tall (Screenshot?)" badge at exactly the ratio that got the product
# rejected, and the grid only shows approved products — so the badge could
# never appear. The advisory tier was unreachable by construction.
#
# 1.5 is also an ordinary product photo: a 2:3 portrait shot is 1.5 exactly,
# so bottles, standing figures and portrait packaging were auto-rejected on
# shape alone.
# ── TV colour exemption (temporary) ────────────────────────────────────────
# Scoped by category PATH prefix, not by a frozen list of codes: the tree gains
# subcategories (Smart TVs and Large screen TV are later additions than the
# rest), and a hardcoded list would quietly stop covering them.
#
# The prefix is deliberately narrow. Matching "TV" anywhere in a path catches
# 102 categories including "Books, Movies and Music / DVDs / Reality TV", car
# tuners, TV trays, wall mounts and remotes. Stopping one level higher, at
# "Electronics / Television & Video /", still gives 34 — that tree also holds
# DVD players, VCRs, AV receivers, satellite dishes and projection screens,
# which do have a colour worth declaring.
#
# This is televisions and nothing else: the Televisions node plus its six
# subcategories.
#
# The node itself is matched as well as its children. Products are routinely
# filed on a parent rather than a leaf, and a listing sitting directly in
# "Televisions" not being covered by the televisions exemption would be a gap
# nobody would predict from the toggle's label.
TV_COLOR_EXEMPT_NODE = "Electronics / Television & Video / Televisions"
TV_COLOR_EXEMPT_PREFIX = TV_COLOR_EXEMPT_NODE + " / "

ASPECT_REJECT_TALL = 2.5      # taller than 2.5x its width  -> rejected
ASPECT_REJECT_WIDE = 0.4      # wider than 2.5x its height  -> rejected
ASPECT_ADVISORY_TALL = 1.5    # 1.5 - 2.5 -> commentary in the grid only
ASPECT_ADVISORY_WIDE = 0.6    # 0.4 - 0.6 -> commentary in the grid only
