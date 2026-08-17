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
ASPECT_REJECT_TALL = 2.5      # taller than 2.5x its width  -> rejected
ASPECT_REJECT_WIDE = 0.4      # wider than 2.5x its height  -> rejected
ASPECT_ADVISORY_TALL = 1.5    # 1.5 - 2.5 -> commentary in the grid only
ASPECT_ADVISORY_WIDE = 0.6    # 0.4 - 0.6 -> commentary in the grid only


# Sneaker sub-brands and model lines, resolved to the house that owns them.
#
# There is no Jordan company and no Airmax company — they are Nike lines, and a
# listing saying "Airmax 97" or "Air Jordan 4" is making a Nike claim.
#
# Lives here rather than in streamlit_app because both the validation checks and
# the review grid need it. ui_components importing streamlit_app re-executes the
# entry script — Streamlit re-runs every widget in it — which raised duplicate
# element IDs and fragment errors on every grid render.
#
# Deliberately excludes model names that are ordinary words — "campus",
# "samba", "dunk" — because they cannot be told from prose.
SNEAKER_BRAND_ALIASES = {
    "jordan": "nike", "air jordan": "nike", "jumpman": "nike", "af1": "nike",
    "airmax": "nike", "air max": "nike", "airforce": "nike", "air force": "nike",
    "all star": "converse", "all stars": "converse", "chuck taylor": "converse",
    "yeezy": "adidas",
    # Deliberate misspellings, which is a tactic rather than a typo: the seller
    # wants the shopper to read "Nike" and the filter to read something else.
    # Only forms with no other meaning are listed — "adida" and "conver" are
    # not words. "mike" is the exception and is included knowingly: it is a
    # name, but inside the sneaker categories, next to a Nike silhouette, it is
    # not a coincidence. Drop it here if it ever costs a real listing.
    "mike": "nike", "nik": "nike", "n!ke": "nike", "nikee": "nike",
    "conver": "converse", "convers": "converse", "converce": "converse",
    "adida": "adidas", "addidas": "adidas", "@didas": "adidas",
    "timb": "timberland", "timberlan": "timberland",
}


# Audio model names, resolved to the house they belong to.
#
# A seller who wants to hide a fake AirPods/Sony/Samsung listing keeps the
# BRAND field bland — Generic, Fashion, Audio — and puts the model name in the
# title. That title is a claim about who made it, and the ceiling for that
# house should apply just as if the BRAND field said so.
#
# Only distinctive tokens. Words that also mean something else — "tune",
# "live", "tour", "studio" — are excluded because a phone tune or a
# music-tour listing would otherwise pretend to be a JBL under a $30 ceiling.
# What survives here are proper-noun model families a shopper searches for.
AUDIO_MODEL_ALIASES = {
    # Apple
    "airpods": "apple", "airpod": "apple",
    "airpods pro": "apple", "airpods pro 2": "apple", "airpods pro 3": "apple",
    "airpods max": "apple", "airpods 4": "apple", "airpods 3": "apple",
    # Sony
    "wh-1000xm5": "sony", "wh-1000xm4": "sony", "wh-1000xm3": "sony",
    "wf-1000xm5": "sony", "wf-1000xm4": "sony",
    "inzone": "sony", "linkbuds": "sony", "ult wear": "sony",
    # Samsung. Bare "galaxy" too — sellers put it in BRAND as a placeholder
    # for Galaxy Buds Pro the same way others use "Watch" for a fake Rolex.
    "galaxy": "samsung",
    "galaxy buds": "samsung", "galaxy buds pro": "samsung",
    "galaxy buds live": "samsung", "galaxy buds fe": "samsung",
    # Beats
    "beats studio buds": "beats", "beats fit pro": "beats",
    "beats solo": "beats", "beats studio": "beats", "powerbeats": "beats",
    "beats pill": "beats", "beats by dre": "beats", "beats by dr. dre": "beats",
    # Bose
    "quietcomfort": "bose", "soundlink": "bose", "quiet comfort": "bose",
    # JBL — only the distinctive numbered families
    "jbl flip": "jbl", "jbl charge": "jbl", "jbl clip": "jbl",
    "jbl xtreme": "jbl", "jbl boombox": "jbl", "jbl partybox": "jbl",
    "jbl quantum": "jbl",
    # Sony compact model tokens. Buyers search for "1000XM5" or just "XM5",
    # and sellers copy those. The check is only ever consulted inside the
    # audio/speaker/watch categories, so a bare "XM5" cannot match some
    # unrelated product elsewhere in the catalogue.
    "1000xm5": "sony", "1000xm4": "sony", "1000xm3": "sony",
    "xm5": "sony", "xm4": "sony",
    # JBL model tokens without the "jbl" prefix. "Flip 6", "Charge 5" and
    # friends inside the audio/speaker categories are unambiguously JBL — a
    # bathroom flip is not in the speaker categories and would not reach here.
    "flip 6": "jbl", "flip 7": "jbl", "flip 5": "jbl",
    "charge 5": "jbl", "charge 6": "jbl",
    "clip 4": "jbl", "clip 5": "jbl",
    "xtreme 3": "jbl", "xtreme 4": "jbl",
    "boombox 3": "jbl", "boombox 2": "jbl",
    "go 4": "jbl", "go 5": "jbl",
    # Bose compact tokens. "QC45" and "QC35" are how the QuietComfort series
    # is written on price tags and in reviews.
    "quietcomfort 45": "bose", "quietcomfort ultra": "bose",
    "qc45": "bose", "qc35": "bose",
    "soundlink flex": "bose", "soundlink mini": "bose", "soundlink micro": "bose",
    # Harman Kardon and its models. HK is its own brand — it appears as a
    # column in the sheet too — but a listing pairing "Harman Kardon" with a
    # JBL model name ("Harman Kardon Flip 6", "Harman Kardon Charge 5") is
    # still a JBL claim, and the JBL alias above catches those on the JBL
    # side. This side gets HK's own model families.
    "onyx studio": "harman kardon", "aura studio": "harman kardon",
    "go play": "harman kardon", "go + play": "harman kardon",
    "citation": "harman kardon", "soundsticks": "harman kardon",
    "esquire mini": "harman kardon",
    # Sennheiser
    "momentum true wireless": "sennheiser", "accentum": "sennheiser",
    # Marshall
    "marshall major": "marshall", "marshall monitor": "marshall",
    "marshall motif": "marshall", "marshall minor": "marshall",
    "marshall middleton": "marshall",
}


# Luxury-watch model names, resolved to the house.
#
# Same reasoning as the audio aliases: a seller writes BRAND=Watch or
# BRAND=Fashion and puts "Daytona" or "Royal Oak" in the title. Only
# distinctive tokens survive — "explorer" alone is too generic and is left
# out, though "explorer ii" is safe. "Tank" is left out for the same reason
# (it is a word); the Cartier ceiling still applies via BRAND=Cartier or when
# "cartier" appears in the name.
WATCH_MODEL_ALIASES = {
    # Rolex
    "daytona": "rolex", "submariner": "rolex", "datejust": "rolex",
    "day-date": "rolex", "day date": "rolex",
    "gmt-master": "rolex", "gmt master": "rolex",
    "yacht-master": "rolex", "yacht master": "rolex",
    "sea-dweller": "rolex", "sea dweller": "rolex",
    "sky-dweller": "rolex", "sky dweller": "rolex",
    "oyster perpetual": "rolex", "explorer ii": "rolex",
    # Patek Philippe
    "nautilus": "patek philippe", "aquanaut": "patek philippe",
    "calatrava": "patek philippe",
    # Audemars Piguet
    "royal oak": "audemars piguet", "royal oak offshore": "audemars piguet",
    # Omega
    "speedmaster": "omega", "seamaster": "omega", "constellation": "omega",
    "aqua terra": "omega", "planet ocean": "omega",
    # Cartier — leaving "tank" out, keeping the distinctive ones
    "ballon bleu": "cartier", "santos de cartier": "cartier",
    # Breitling
    "navitimer": "breitling", "chronomat": "breitling",
    "superocean": "breitling", "avenger": "breitling",
    # Hublot
    "big bang": "hublot", "classic fusion": "hublot",
    # TAG Heuer — only distinct compound names; "carrera" alone is a car
    "tag heuer carrera": "tag heuer", "aquaracer": "tag heuer",
    "tag heuer monaco": "tag heuer",
    # Richard Mille
    "richard mille": "richard mille",  # its own name — helps when BRAND is
                                       # empty and the seller only writes it
                                       # in the title
    # Casio watches. G-Shock is a Casio sub-brand and sellers write it as
    # either "G-Shock", "Gshock" or "G Shock"; each is really a Casio claim.
    # Baby-G, Edifice, Pro Trek and Oceanus are the other well-known lines.
    "g-shock": "casio", "gshock": "casio", "g shock": "casio",
    "baby-g": "casio", "baby g": "casio", "babyg": "casio",
    "edifice": "casio", "pro trek": "casio", "protrek": "casio",
    "oceanus": "casio",
}


# Casio calculator model families. fx-991EX ClassWiz is the counterfeit
# target — genuine ~$20, fakes ~$3-5 — and sellers write it many ways: with
# or without hyphen, with "ClassWiz" alone, with the plain "fx-991".
CALCULATOR_MODEL_ALIASES = {
    "fx-991ex": "casio", "fx991ex": "casio", "fx-991": "casio", "fx991": "casio",
    "fx-991es": "casio", "fx-991spx": "casio",
    "fx-570ex": "casio", "fx-570es": "casio",
    "fx-115es": "casio", "fx-115": "casio",
    "fx-9750giii": "casio", "fx-9750": "casio",
    "fx-9860giii": "casio", "fx-cg50": "casio", "fx-cg500": "casio",
    "classwiz": "casio",
}


# Every model → parent-brand map, merged. `check_suspected_fake_products`
# iterates this to give a listing that mentions a model but not its brand the
# ceiling of the brand that owns the model.
PRICE_CEILING_MODEL_ALIASES = {
    **SNEAKER_BRAND_ALIASES,
    **AUDIO_MODEL_ALIASES,
    **WATCH_MODEL_ALIASES,
    **CALCULATOR_MODEL_ALIASES,
}
