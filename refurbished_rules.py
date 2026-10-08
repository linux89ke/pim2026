"""Refurbished Products Validation Rules (REF-01 to REF-11)

Implements the rulebook specification from refurbished_listing_rulebook.xlsx / .docx:
- REF-11: Already-Compliant Suppression Guard (evaluated first)
- REF-07: Component / Accessory Guard
- REF-08: Current-Generation Model Guard
- REF-04: Refurb Terms Outside Allowed Scope (Laptops, Phones, Tablets)
- REF-06: Discontinued Device Marked 'Brand New' / 'Sealed'
- REF-10: Seller Name as Brand
- REF-09: Generic / Unmapped / Blank Brand on Refurb Item
- REF-01: OEM Brand Instead of Refurb Brand
- REF-05: Discontinued / Out-of-Market Device Not Marked Refurbished
- REF-02: Misspelled / Non-Standard Refurb Terms in Title
- REF-03: Brand = Renewed/Refurbished but Title Missing 'Refurbished'
"""

from __future__ import annotations

import re
from typing import Dict, List, Optional, Set, Tuple
import pandas as pd

_REFURB_BRAND_OVERTURNED_STAGING: dict = {}

# ----------------------------------------------------------------------
# Standard Jumia Reject Reasons
# ----------------------------------------------------------------------
REASON_BRAND_NOT_ALLOWED = "1000001 - Brand NOT Allowed"
REASON_OTHER_REASON = "1000007 - Other Reason"
REASON_IMPROVE_NAME = "1000008 - Kindly Improve Product Name Description"
REASON_SELLER_SUPPORT = (
    "1000028 - Kindly Contact Jumia Seller Support To Confirm Possibility Of Sale Of This Product By Raising A Claim"
)

# ----------------------------------------------------------------------
# Approved category codes for refurbished PC/Laptop/Phone/Monitor listings
# Sourced directly from Refurb.xlsx ('Categories' sheet: Phones + Laptops)
# ----------------------------------------------------------------------
REFURB_ELIGIBLE_CATEGORY_CODES: frozenset = frozenset({
    # --- Phones & Tablets (18 categories from Refurb.xlsx) ---
    "1002282",  # Phones & Tablets / Mobile Phones / Smartphones
    "1002300",  # Phones & Tablets / Mobile Phones / Smartphones / Android Phones
    "1002314",  # Phones & Tablets / Mobile Phones / Smartphones / iOS Phones
    "1002333",  # Phones & Tablets / Mobile Phones / Smartphones / Other Operating Systems
    "1002345",  # Phones & Tablets / Mobile Phones / Smartphones / Windows Phones
    "1002358",  # Phones & Tablets / Phone & Fax
    "1002494",  # Phones & Tablets / Phone & Fax / Cell Phone Unit
    "1002517",  # Phones & Tablets / Phone & Fax / Cell Phone Unit / Analog Phone
    "1002532",  # Phones & Tablets / Phone & Fax / Cell Phone Unit / Digital Phone
    "1002543",  # Phones & Tablets / Phone & Fax / Cell Phone Unit / VOIP Phone
    "1002562",  # Phones & Tablets / Phone & Fax / Fax Console
    "1002581",  # Phones & Tablets / Phone & Fax / Prepaid Phone Cards
    "1002995",  # Phones & Tablets / Tablets
    "1029505",  # Phones & Tablets / Tablets / Educational Tablets
    "1003009",  # Phones & Tablets / Tablets / iPad Tablets
    "1003018",  # Phones & Tablets / Tablets / Other Tablets
    "1029506",  # Phones & Tablets / Tablets / Professional Tablets
    "1003433",  # Phones & Tablets / Unlocked Cell Phones
    # Additional legacy mobile phone codes
    "1002209",  # Carrier Cell Phones
    "1002221",  # Mobile Phones
    "1002235",  # Cell Phones
    "1002252",  # Cell Phones

    # --- Laptops & Computing (16 categories from Refurb.xlsx) ---
    "1000018",  # Computing
    "1003043",  # Computing / Computers & Accessories
    "1003690",  # Computing / Computers & Accessories / Computers & Tablets
    "1003704",  # Computing / Computers & Accessories / Computers & Tablets / Desktops
    "1003723",  # Computing / Computers & Accessories / Computers & Tablets / Desktops / All-in-Ones
    "1003747",  # Computing / Computers & Accessories / Computers & Tablets / Desktops / Minis
    "1003770",  # Computing / Computers & Accessories / Computers & Tablets / Desktops / Towers
    "1003787",  # Computing / Computers & Accessories / Computers & Tablets / Laptops
    "1003803",  # Computing / Computers & Accessories / Computers & Tablets / Laptops / 2 in 1 Laptops
    "1029459",  # Computing / Computers & Accessories / Computers & Tablets / Laptops / Gaming Laptops
    "1003815",  # Computing / Computers & Accessories / Computers & Tablets / Laptops / Traditional Laptops
    "1003831",  # Computing / Computers & Accessories / Computers & Tablets / Laptops / Traditional Laptops / Macbooks
    "1003845",  # Computing / Computers & Accessories / Computers & Tablets / Laptops / Traditional Laptops / Netbooks
    "1029467",  # Computing / Computers & Accessories / Computers & Tablets / Laptops / Traditional Laptops / Notebooks
    "1003860",  # Computing / Computers & Accessories / Computers & Tablets / Laptops / Traditional Laptops / Ultrabooks
    "1008462",  # Computing / Monitors (in Refurb.xlsx Categories)
    "1004361",  # Electronics / Television & Video / Monitors
})

# ----------------------------------------------------------------------
# Term Regexes & Dictionaries
# ----------------------------------------------------------------------
CANONICAL_REFURB_TERMS = {"refurbished", "renewed"}

# Approved brand values for refurbished goods
APPROVED_REFURB_BRANDS = {"refurbished", "renewed", "refurbished / renewed", "apple refurbished"}

# Non-standard refurb terms, misspellings, abbreviations, sourcing tags (REF-02)
NONSTANDARD_REFURB_RE = re.compile(
    r"\b("
    r"REFURBLISHED|RREFUBRISHED|REBURBISHED|RUFURBISHED|REFURBRISHED|Reburbished|reurbished|"
    r"REFURBISED|REFURBSHED|REFURBISHD|REFURBISHHED|"
    r"REFURB|"
    r"EX[- _]?UK|EXUK|"
    r"PRE[- ]?OWNED|PREOWNED|"
    r"PRE[- ]?LOVED|PRELOVED|"
    r"SECOND[- ]?HAND|2ND\s?HAND|"
    r"USED(?:\s+LIKE\s+NEW)?|"
    r"RECONDITIONED|"
    r"GRADE\s?[ABC]|"
    r"OPEN[- ]?BOX|"
    r"LIKE[- ]?NEW|"
    r"OEM[- ]?PULL(?:ED)?|"
    r"PULLED"
    r")\b",
    re.IGNORECASE,
)

# All refurb indicators (standard + non-standard) for scoping
ALL_REFURB_INDICATORS_RE = re.compile(
    r"\b("
    r"REFURBISHED|RENEWED|"
    r"REFURBLISHED|RREFUBRISHED|REBURBISHED|RUFURBISHED|REFURBRISHED|Reburbished|reurbished|"
    r"REFURBISED|REFURBSHED|REFURBISHD|REFURBISHHED|"
    r"REFURB|"
    r"EX[- _]?UK|EXUK|"
    r"PRE[- ]?OWNED|PREOWNED|"
    r"PRE[- ]?LOVED|PRELOVED|"
    r"SECOND[- ]?HAND|2ND\s?HAND|"
    r"USED(?:\s+LIKE\s+NEW)?|"
    r"RECONDITIONED|"
    r"GRADE\s?[ABC]|"
    r"OPEN[- ]?BOX|"
    r"LIKE[- ]?NEW|"
    r"OEM[- ]?PULL(?:ED)?|"
    r"PULLED"
    r")\b",
    re.IGNORECASE,
)

# Brand new / Sealed claims (REF-06)
BRAND_NEW_CLAIM_RE = re.compile(
    r"\b(BRAND\s?NEW|SEALED|\(NEW\))\b",
    re.IGNORECASE,
)

# Grammatical verb usages of 'used' (e.g. 'used as a peeler', 'can be used for cooking')
# These must NEVER be treated as condition terms!
VERB_USED_RE = re.compile(
    r"(?:"
    r"\b(?:can|could|may|might|should|would|will|to|be|being|been|is|are|was|were|widely|commonly|frequently|rarely|easily|also|never|not|both)\s+used\b"
    r"|"
    r"\bused\s+(?:as|for|in|into|to|with|by|on|when|while|during|at|around|within)\b"
    r")",
    re.IGNORECASE,
)

# Component / Accessory Guard (REF-07)
COMPONENT_ACCESSORY_GUARD_RE = re.compile(
    r"\b("
    r"LCD\s?INVERTER|SCREEN\s?ONLY|CASING\s?ONLY|BATTERY\s?ONLY|STYLUS\s?ONLY|"
    r"SPARE\s?PART|REPLACEMENT\s?(?:SCREEN|BATTERY|KEYBOARD|HOUSING|GLASS|CASING|MOTHERBOARD|FAN|CHARGER|ADAPTER)|"
    r"KEYBOARD\s?FOR\b|BATTERY\s?FOR\b|CHARGER\s?FOR\b|SCREEN\s?FOR\b|ADAPTER\s?FOR\b"
    r")\b",
    re.IGNORECASE,
)

# Major OEM Brands that should be mapped to Refurbished when selling refurb/EOL devices (REF-01)
OEM_BRANDS: Set[str] = {
    "hp", "dell", "lenovo", "apple", "samsung", "asus", "acer",
    "huawei", "xiaomi", "google", "microsoft", "infinix", "tecno",
    "nokia", "amazon", "toshiba", "nec", "hewlett packard", "sony",
    "fujitsu", "alienware", "msi", "lg", "htc", "motorola", "oneplus",
    "realme", "vivo", "blackberry", "panasonic", "itel", "oppo"
}

# Generic / Blank brand indicators (REF-09)
GENERIC_BRANDS: Set[str] = {
    "", "generic", "generique", "fashion", "other", "autre", "nan", "none"
}

# ----------------------------------------------------------------------
# Discontinued / Out-of-Market Models (Master List with REF-08 Guards)
# Source: out_of_market_devices_master_with_desktops.xlsx
# ----------------------------------------------------------------------
DISCONTINUED_MODELS_CONFIG: List[Tuple[str, re.Pattern, str]] = [

    # ------------------------------------------------------------------
    # HP Laptops, Desktops, Workstations & Monitors
    # ------------------------------------------------------------------
    (
        "HP EliteBook Series",
        re.compile(r"\b(?:HP\s*)?EliteBook\s*(?:x360\s*)?(?:[678][0-9][05]\s*G[1-9]|8[34]0\s*G10|835\s*G8|845\s*G7|745\s*G6|10[34]0\s*G[2-7]|Folio\s*(?:1020|9[24][78]0m?))\b", re.IGNORECASE),
        "HP EliteBook series",
    ),
    (
        "HP ProBook Series",
        re.compile(r"\b(?:HP\s*)?ProBook\s*(?:[46][0-9][05]\s*G[1-9]|x360\s*11\s*G[34]\s*EE)\b", re.IGNORECASE),
        "HP ProBook series",
    ),
    (
        "HP Pavilion/Spectre/Envy/Dragonfly (retired)",
        re.compile(r"\b(?:HP\s*)?(?:Pavilion|Spectre|Envy|Dragonfly)\b", re.IGNORECASE),
        "HP Pavilion/Spectre/Envy/Dragonfly (retired brand)",
    ),
    (
        "HP ZBook Series",
        re.compile(r"\b(?:HP\s*)?ZBook\s*(?:14u\s*G5|15\s*G[3-5]|Studio\s*G3)\b", re.IGNORECASE),
        "HP ZBook series",
    ),
    (
        "HP 250 / 14/15 series",
        re.compile(r"\bHP\s*(?:250\s*G[5-8]|1[45]-d[aw])\b", re.IGNORECASE),
        "HP 250 / 14/15 series",
    ),
    (
        "HP ProDesk / EliteDesk Desktops",
        re.compile(r"\b(?:HP\s*)?(?:ProDesk\s*[46]00|EliteDesk\s*800)\s*G[1-6](?:\s*(?:SFF|MT|Tower|Mini))?\b", re.IGNORECASE),
        "HP ProDesk / EliteDesk G1–G6",
    ),
    (
        "HP Compaq Desktops",
        re.compile(r"\b(?:HP\s*)?Compaq\s*(?:6000|6200|6300|8200|8300|Desktop|CPU)?(?:\s*(?:Pro|Elite|Desktop|CPU))?\b", re.IGNORECASE),
        "HP Compaq Pro/Elite",
    ),
    (
        "HP All-in-One & Workstations",
        re.compile(r"\b(?:HP\s*)?(?:ProOne\s*[46]00|EliteOne\s*800|Z[246][1-4]0|Z2\s*Mini\s*G5)\b", re.IGNORECASE),
        "HP AIO / Workstation",
    ),
    (
        "HP Monitors",
        re.compile(r"\b(?:HP\s*)?(?:EliteDisplay\s*E2[0-4][1-3][a-z]?|ProDisplay\s*P2[24]1[a-z]?|ZR2[0-9]{1,3}[a-z]*)\b", re.IGNORECASE),
        "HP Monitor",
    ),

    # ------------------------------------------------------------------
    # Lenovo ThinkPad, ThinkCentre, IdeaPad, Desktops & Monitors
    # ------------------------------------------------------------------
    (
        "Lenovo ThinkPad Series",
        re.compile(r"\b(?:Lenovo\s*)?ThinkPad\s*(?:X2[0-8]0|X390|X13\s*Gen\s*[12]|T4[3-9][05]s?|T495s?|T1[45]\s*Gen\s*[1-3]|L[34][4-9]0|X1\s*Carbon\s*(?:Gen\s*)?[1-8]|T25|11e|Yoga\s*11e|P5[0-3]|P14s\s*Gen\s*1|E4[89]0|E14\s*Gen\s*[12])\b", re.IGNORECASE),
        "Lenovo ThinkPad series",
    ),
    (
        "Lenovo IdeaPad / Yoga Series",
        re.compile(r"\b(?:Lenovo\s*)?(?:IdeaPad\s*(?:[135]00|Flex\s*[56]|S[35]40|D330)|Yoga\s*(?:[23]|7[123]0|9[012]0))\b", re.IGNORECASE),
        "Lenovo IdeaPad / Yoga",
    ),
    (
        "Lenovo ThinkCentre Desktops & AIO",
        re.compile(r"\b(?:Lenovo\s*)?ThinkCentre\s*M(?:72e|73|83|92p|93p|700|800|900|710t|720t|910t|920t|72z|92z|93z|800z|900z|910z)(?:\s*(?:SFF|Tower|Tiny|AIO))?\b", re.IGNORECASE),
        "Lenovo ThinkCentre",
    ),
    (
        "Lenovo ThinkStation / IdeaCentre Desktops",
        re.compile(r"\b(?:Lenovo\s*)?(?:ThinkStation\s*P[35][012]0|IdeaCentre\s*(?:300|500|[35]))\b", re.IGNORECASE),
        "Lenovo ThinkStation / IdeaCentre",
    ),
    (
        "Lenovo ThinkVision Monitors",
        re.compile(r"\b(?:Lenovo\s*)?ThinkVision\s*(?:LT[12][92]52p|T2[234][0-9a-z-]+)\b", re.IGNORECASE),
        "Lenovo ThinkVision Monitor",
    ),
    (
        "Lenovo Tab Series",
        re.compile(r"\b(?:Lenovo\s*)?Tab\s*(?:[234]|M[0-9]+|P1[01])\b", re.IGNORECASE),
        "Lenovo Tab",
    ),

    # ------------------------------------------------------------------
    # Dell Latitude, OptiPlex, Precision, Inspiron & Monitors
    # ------------------------------------------------------------------
    (
        "Dell Latitude Series",
        re.compile(r"\b(?:Dell\s*)?Latitude\s*(?:E?[3-7][0-9x]{3}|[3-7]xxx|3189|3190|3380|3490|3590|5420|5480|5490|5580|5590|7280|7300|7390|7480|7490)\b", re.IGNORECASE),
        "Dell Latitude series",
    ),
    (
        "Dell Inspiron / XPS / Studio Laptops",
        re.compile(r"\b(?:Dell\s*)?(?:Inspiron\s*1[3457]\s*[35]000|XPS\s*1[35]\b.*?\b201[5-8]|Studio\s*(?:XPS\s*)?(?:1[0-9]{3}|M1[0-9]{3})|Adamo(?:\s*XPS)?|Chromebook\s*31[28]0)\b", re.IGNORECASE),
        "Dell Inspiron/XPS/Studio",
    ),
    (
        "Dell OptiPlex Desktops & AIO",
        re.compile(r"\b(?:Dell\s*)?OptiPlex\s*(?:30[1-7]0|50[4-7]0|70[124-7]0|90[12]0|3011\s*AIO|5250\s*AIO|74[4-6]0\s*AIO)(?:\s*(?:SFF|MT|Micro|Tower))?\b", re.IGNORECASE),
        "Dell OptiPlex series",
    ),
    (
        "Dell Precision Workstations & Vostro / Inspiron Desktops",
        re.compile(r"\b(?:Dell\s*)?(?:Precision\s*T(?:1650|1700|36[01]0|5600|5810|7810)|Precision\s*(?:35[12]0|75[12]0)|Vostro\s*(?:3400|3500|3590|5471)|Inspiron\s*(?:3250|3650|3847|3470)\s*Desktop)\b", re.IGNORECASE),
        "Dell Precision/Vostro/Inspiron Desktop",
    ),
    (
        "Dell UltraSharp & P/E Series Monitors",
        re.compile(r"\b(?:Dell\s*)?(?:UltraSharp\s*U2[247]1[0-5][A-Z]*|P2[0-4]1[49]H|E1913H|E2216H)\b", re.IGNORECASE),
        "Dell Monitor",
    ),

    # ------------------------------------------------------------------
    # Apple MacBooks, Desktops, Monitors & Discontinued iPhones/iPads
    # REF-08 guard: current lineup (iPhone 15/16/17) must not be matched
    # ------------------------------------------------------------------
    (
        "Apple MacBooks",
        re.compile(r"\bMacBook\s*(?:Pro|Air|12[- ]inch)?\b.*?\b(?:201[0-9]|2020|Touch\s*Bar|M[123])\b", re.IGNORECASE),
        "Apple MacBook",
    ),
    (
        "Apple iMac / Mac mini / Studio / Pro",
        re.compile(r"\b(?:iMac\s*(?:20|21\.5|24|27)[- ]inch|iMac\b.*?\b(?:Retina\s*5K|Unibody|Slim|Aluminum|200[7-9]|201[0-9]|2020|M1|M3)|Mac\s*mini\b.*?\b(?:Intel|201[0-8]|M1|M2)|Mac\s*Studio\b.*?\bM[12]|Mac\s*Pro\b.*?\b(?:Tower|1\.1|5\.1|200[6-9]|201[239]))\b", re.IGNORECASE),
        "Apple Mac Desktop",
    ),
    (
        "Apple Cinema & Thunderbolt Displays",
        re.compile(r"\bApple\s*(?:(?:LED\s*)?Cinema\s*Display|Thunderbolt\s*Display)\b", re.IGNORECASE),
        "Apple Cinema / Thunderbolt Display",
    ),
    (
        "Apple iPhones (pre-15)",
        re.compile(r"\biPhone(?:\s*(?:Base\b|3G[S]?|4S?|5[sc]?|6\s*Plus|6s(?:\s*Plus)?|7\s*Plus|[3-8]|X[RS]?|1[1-4]))(?:\s*(?:Pro|Max|Pro\s*Max|Plus|Mini|Base|s|c))?\b(?!\s*1[5-7])", re.IGNORECASE),
        "iPhone 3–14 series",
    ),
    (
        "Apple iPhone SE",
        re.compile(r"\biPhone\s*SE\s*(?:[1-3](?:st|nd|rd)?\s*gen|(?:2016|2020|2022))\b", re.IGNORECASE),
        "iPhone SE",
    ),
    (
        "Apple iPad (discontinued generations)",
        re.compile(r"\biPad\s*(?:Air\s*[1-5]|Mini\s*[1-6]|Pro\s*(?:201[5-9]|202[0-3])|[1-9](?:st|nd|rd|th)?\s*gen)?\b", re.IGNORECASE),
        "iPad (discontinued gens)",
    ),

    # ------------------------------------------------------------------
    # Samsung Galaxy Series (S, Note, Fold, Flip, Tab, A, J, M)
    # ------------------------------------------------------------------
    (
        "Samsung Galaxy S series",
        re.compile(r"\b(?:Samsung\s*)?Galaxy\s*S?(?:2[0-3]|10[e+]?|[1-9])(?!\d)\b", re.IGNORECASE),
        "Galaxy S series",
    ),
    (
        "Samsung Galaxy Note series",
        re.compile(r"\b(?:Samsung\s*|Galaxy\s*)?Note\s*(?:2[0-9]|10|[1-9](?:\.1)?)(?:\s*(?:Plus|Ultra|FE|\+|Lite|5G|Base))?\b", re.IGNORECASE),
        "Galaxy Note series",
    ),
    (
        "Samsung Galaxy Z Fold / Z Flip",
        re.compile(r"\b(?:Samsung\s*)?(?:Galaxy\s*)?Z\s*(?:Flip|Fold)\s*[1-5]?\b", re.IGNORECASE),
        "Galaxy Z Flip/Fold",
    ),
    (
        "Samsung Galaxy Tab series",
        re.compile(r"\b(?:Samsung\s*)?Galaxy\s*Tab\s*(?:A|S[2-6]e?)\b", re.IGNORECASE),
        "Samsung Galaxy Tab",
    ),
    (
        "Samsung Galaxy A / J / M series",
        re.compile(r"\b(?:Samsung\s+Galaxy|Samsung|Galaxy)\s+(?:A[3-9]|A(?:[1-3][0-4]|5[0-4]|70)|J[1-9]|M(?:10|20|30|31|51))(?:\s*(?:Plus|s|\+|5G))?\b(?!\d)", re.IGNORECASE),
        "Galaxy A/J/M series",
    ),
    (
        "Samsung Legacy Monitors & AIO",
        re.compile(r"\b(?:Samsung\s*)?(?:SyncMaster\s*(?:B2230|S22B300|S24D300)|Smart\s*Monitor\s*M[57]|All-in-One\s*PC\s*Series\s*[57])\b", re.IGNORECASE),
        "Samsung Monitor/AIO",
    ),

    # ------------------------------------------------------------------
    # Google Pixel & Nexus
    # ------------------------------------------------------------------
    (
        "Google Pixel 1–8 / Fold",
        re.compile(r"\b(?:Google\s*)?Pixel\s*(?:[1-8]a?|XL|Fold|[1-8]\s*(?:XL|Pro)?)\b", re.IGNORECASE),
        "Google Pixel 1–8 / Fold",
    ),
    (
        "Google Nexus / Pixel Tablet / Pixelbook / Slate",
        re.compile(r"\b(?:Nexus\s*(?:One|S|[4-6]|5X|6P)|Galaxy\s*Nexus|Pixel\s*Tablet|Pixelbook(?:\s*Go)?|Pixel\s*Slate)\b", re.IGNORECASE),
        "Google Nexus / Pixelbook",
    ),

    # ------------------------------------------------------------------
    # Huawei Mobile
    # ------------------------------------------------------------------
    (
        "Huawei Ascend / P / Mate / Nova",
        re.compile(r"\b(?:Huawei\s*)?(?:Ascend\s*(?:M680|II|G[0-9]{3}s?|G6|G620s)|P(?:10|20|30|40(?:\s*(?:lite|Pro|Pro\+|Plus))?|50(?:\s*(?:Pro|Pocket))?)|Mate\s*(?:10|20|30|X[s]?)|Nova\s*[67])\b", re.IGNORECASE),
        "Huawei P/Mate/Nova/Ascend",
    ),

    # ------------------------------------------------------------------
    # Xiaomi / Redmi / Poco
    # ------------------------------------------------------------------
    (
        "Xiaomi Redmi / Mi / Poco",
        re.compile(r"\b(?:Redmi\s*Note\s*(?:1[0-3]|[3-9])|Mi\s*(?:[5-9]|1[0-3])|Xiaomi\s*1[23]|Redmi\s*[4-7]|Mi\s*A[1-3]|Poco\s*(?:F[1-3]|X[34]\s*Pro|M[34]\s*Pro|[FXM][1-4]))\b", re.IGNORECASE),
        "Xiaomi Redmi/Mi/Poco",
    ),

    # ------------------------------------------------------------------
    # LG Mobile & Monitors
    # ------------------------------------------------------------------
    (
        "LG Mobile & Monitors",
        re.compile(r"\bLG\s*(?:Wing|Velvet|G[3-8]|V(?:10|20|30|35|40|50|60)|K\s*[A-Za-z0-9]+|Flatron\s*(?:E2242|22M35|24M47D)|UltraWide\s*(?:25UM58|29UM58))\b", re.IGNORECASE),
        "LG Mobile / Monitor",
    ),

    # ------------------------------------------------------------------
    # Sony Xperia
    # ------------------------------------------------------------------
    (
        "Sony Xperia",
        re.compile(r"\b(?:Sony\s*)?Xperia\s*(?:Z[0-9]?|X[AZ]?[0-9]?|XZ[0-9]?|(?:1|5|10)(?:\s*(?:II|III))?)\b", re.IGNORECASE),
        "Sony Xperia",
    ),

    # ------------------------------------------------------------------
    # Oppo
    # ------------------------------------------------------------------
    (
        "Oppo A / Find / Reno",
        re.compile(r"\bOppo\s*(?:A(?:3[01]?|5s?|9|1[2456]|[5-7][04])|Find\s*X[0-3]?|Reno(?:\s*[1-5])?)\b", re.IGNORECASE),
        "Oppo A/Find/Reno",
    ),

    # ------------------------------------------------------------------
    # Motorola
    # ------------------------------------------------------------------
    (
        "Motorola Moto / Razr",
        re.compile(r"\b(?:Motorola\s*)?(?:Moto\s*[GEZ]\s*[0-9]+|Razr(?:\s*\((?:2019)\)|\s*2019|\s*5G|\s*40)?)\b", re.IGNORECASE),
        "Motorola Moto/Razr",
    ),

    # ------------------------------------------------------------------
    # OnePlus
    # ------------------------------------------------------------------
    (
        "OnePlus Series",
        re.compile(r"\b(?:OnePlus\s*(?:One|2|3T?|[5-9]T?|10\s*Pro|11)|(?:OnePlus\s*)?Nord(?:\s*(?:[1-3]|N10\s*5G|N100|N200|CE(?:\s*[1-3])?))?)\b", re.IGNORECASE),
        "OnePlus",
    ),

    # ------------------------------------------------------------------
    # Realme
    # ------------------------------------------------------------------
    (
        "Realme Series",
        re.compile(r"\bRealme\s*(?:10|[1-9]|C(?:11|15|21|35)|GT(?:\s*Neo\s*2)?)\b", re.IGNORECASE),
        "Realme",
    ),

    # ------------------------------------------------------------------
    # Transsion (Tecno, Infinix, Itel)
    # ------------------------------------------------------------------
    (
        "Tecno Spark & Camon",
        re.compile(r"\b(?:Tecno\s*)?(?:Spark\s*(?:10|[1-9])|Camon\s*(?:1[2-9]|20))\b", re.IGNORECASE),
        "Tecno Spark/Camon",
    ),
    (
        "Infinix Hot & Note",
        re.compile(r"\b(?:Infinix\s*)?(?:Hot\s*(?:[7-9]|1[0-2])|Note\s*(?:[7-9]|1[0-2]))\b", re.IGNORECASE),
        "Infinix Hot/Note",
    ),
    (
        "Itel A & P Series",
        re.compile(r"\b(?:Itel\s*)?(?:A100C|A200|A90|A10|P38)\b", re.IGNORECASE),
        "Itel",
    ),

    # ------------------------------------------------------------------
    # Vivo
    # ------------------------------------------------------------------
    (
        "Vivo Series",
        re.compile(r"\bVivo\s*(?:V(?:11|15|17|19|2[0135])|Y(?:1[1257]|20|30|50)|NEX\s*3?|X(?:50|60|70|80))\b", re.IGNORECASE),
        "Vivo",
    ),

    # ------------------------------------------------------------------
    # Nokia
    # ------------------------------------------------------------------
    (
        "Nokia Lumia & Android",
        re.compile(r"\b(?:Nokia\s+(?:Lumia\s*(?:520|625|720|920|1020|1520)|[235678]\.1(?:\s*Plus)?|9\s*PureView|[GX][12]0)|Lumia\s*(?:520|625|720|920|1020|1520))\b", re.IGNORECASE),
        "Nokia Lumia/Android",
    ),

    # ------------------------------------------------------------------
    # HTC & BlackBerry
    # ------------------------------------------------------------------
    (
        "HTC & BlackBerry",
        re.compile(r"\b(?:HTC\s*(?:One\s*M[789]|10|U11|U12\+?)|BlackBerry\s*(?:Bold\s*9[09]00|Curve\s*8520|Z10|Q10|Priv|Key[Oo]ne|Key2))\b", re.IGNORECASE),
        "HTC / BlackBerry",
    ),

    # ------------------------------------------------------------------
    # Asus & Acer Desktops & Monitors
    # ------------------------------------------------------------------
    (
        "Asus / Acer Desktops & Monitors",
        re.compile(r"\b(?:Asus\s*(?:ZenBook\s*UX3(?:03|05|10)|VivoPC\s*VM[46]0[A-Z]?|ROG\s*G(?:R8|20)|VS2[24][0-9]H|VP228H)|Acer\s*(?:Veriton\s*[XMN][24]630G|Aspire\s*C2[24]\s*AIO|V196HQL|K222HQL|TravelMate\s*P[24]|Aspire\s*[357]))\b", re.IGNORECASE),
        "Asus / Acer",
    ),

    # ------------------------------------------------------------------
    # Chromebooks
    # ------------------------------------------------------------------
    (
        "Chromebooks",
        re.compile(r"\b(?:HP|Acer|Lenovo|Dell|Samsung|Asus)\s*Chromebook\b", re.IGNORECASE),
        "Chromebook",
    ),

    # ------------------------------------------------------------------
    # Other Laptops (Microsoft Surface, Toshiba, Fujitsu, Panasonic)
    # ------------------------------------------------------------------
    (
        "Other Laptops (Surface/Toshiba/Fujitsu/Panasonic)",
        re.compile(r"\b(?:Surface\s*(?:Laptop\s*[1-5]|Book\s*[1-3]|Go\s*[1-3])|Toshiba\s*(?:Tecra\s*A50|Port[eé]g[eé]\s*Z30|Dynabook\s*B65)|Fujitsu\s*Life[bB]ook\s*(?:U748|E5410)|Panasonic\s*Toughbook\s*CF[- ]?[1-5][0-9]?)\b", re.IGNORECASE),
        "Other Laptops",
    ),

    # ------------------------------------------------------------------
    # Tablets & Mini PCs
    # ------------------------------------------------------------------
    (
        "Tablets & Mini PCs",
        re.compile(r"\b(?:Surface\s*Pro\s*[3-7]|HP\s*Elite\s*x2\s*(?:1012|G4)|NEC\s*VersaPro\s*Tablet|(?:HP\s*(?:EliteDesk|ProDesk)|Dell\s*OptiPlex|Lenovo\s*ThinkCentre|NEC|Generic)\s*Mini\s*PC|OptiPlex\s*Micro|EliteDesk\s*Mini|ProDesk\s*Mini|ThinkCentre\s*Tiny)\b", re.IGNORECASE),
        "Tablets & Mini PCs",
    ),

    # ------------------------------------------------------------------
    # Gaming Consoles (Out of market hardware)
    # ------------------------------------------------------------------
    (
        "Gaming Consoles",
        re.compile(r"\b(?:PSP(?:\s*(?:1000|2000|3000|Go|E1000))?|PS\s*Vita|PlayStation\s*[1-4]|PS[1-4]\b|Xbox\s*(?:360|One(?:\s*[SX])?)|Nintendo\s*(?:Wii(?:\s*U)?|[23]DS(?:\s*XL)?|DS(?:\s*(?:Lite|i|i\s*XL))?|Switch(?:\s*(?:Lite|OLED))?)|Sega\s*(?:Genesis|Dreamcast)|Game\s*Boy(?:\s*(?:Color|Advance))?)\b", re.IGNORECASE),
        "Gaming Console",
    ),
]

# Guard to avoid mistaking game software discs, controllers, and accessories for console hardware
GAME_SOFTWARE_ACCESSORY_GUARD_RE = re.compile(
    r"\b("
    r"games?|discs?|disks?|cd|dvd|cartridges?|gta|grand\s?theft\s?auto|call\s?of\s?duty|fifa|pes|mortal\s?kombat|"
    r"far\s?cry|assassin|tomb\s?raider|halo|forza|battlefield|need\s?for\s?speed|resident\s?evil|"
    r"god\s?of\s?war|uncharted|spider-?man|gran\s?turismo|avengers|insurgency|creed|wwe|"
    r"controllers?|gamepads?|game\s?pads?|joysticks?|steerings?|wheels?|driving\s?force|"
    r"charging|charge\s?kit|battery\s?pack|batteries|casing|protective|skins?|power\s?supply|cables?|adapters?|"
    r"supports\s?ps|compatible\s?with|retro\s?m8|retro\s?handheld|built[- ]in\s?\d+\s?games|"
    r"for\s+(?:ps[1-5]|xbox|pc|android|ios)"
    r")\b",
    re.IGNORECASE,
)

# Explicit out-of-scope product types for refurb (REF-04)
# (printers, TVs/monitors, smartwatches, home appliances, etc.)
INELIGIBLE_REFURB_PRODUCTS_RE = re.compile(
    r"\b("
    r"PRINTER|LASERJET|INKJET|DESKJET|PHOTOCOPIER|TONER|CARTRIDGE|"
    r"TELEVISION|SMART\s?TV|TV\b|PROJECTOR|"
    r"SMARTWATCH|APPLE\s?WATCH|FITBIT|GARMIN\s?WATCH|"
    r"MICROWAVE|REFRIGERATOR|FRIDGE|BLENDER|KETTLE|VACUUM|WASHING\s?MACHINE"
    r")\b",
    re.IGNORECASE,
)

# Dedicated Samsung out-of-market patterns that match even when 'Galaxy' or 'S' is omitted
# e.g., 'Note 10 256GB Single', 'Note 20 Ultra', 'S10 128GB', 'Galaxy 23 Ultra', 'Z Fold 4'
_SAMSUNG_DISCONTINUED_PATTERNS: List[Tuple[re.Pattern, str]] = [
    (
        re.compile(r"\b(?:(?:Samsung\s*)?Galaxy\s*S?|Samsung\s*S?|S)(2[0-3])\s*(?:Plus|Ultra|FE|\+|Base)?\b", re.IGNORECASE),
        "Galaxy S20–S23 series",
    ),
    (
        re.compile(r"\b(?:(?:Samsung\s*)?Galaxy\s*S?|Samsung\s*S?|S)(10[e+]?|[1-9])(?!\d)\b", re.IGNORECASE),
        "Galaxy S1–S10 / S10e / S10+",
    ),
    (
        re.compile(r"\b(?:(?:Samsung|Galaxy)\s+)?Note\s*(?:2[0-9]|10|[1-9](?:\.1)?)(?:\s*(?:Plus|Ultra|FE|\+|Lite|5G|Base))?\b", re.IGNORECASE),
        "Galaxy Note series",
    ),
    (
        re.compile(r"\b(?:(?:Samsung\s*)?(?:Galaxy\s*)?)?(?:Z\s*)?(?:Flip|Fold)\s*[1-5]?(?:\s*(?:5G))?\b", re.IGNORECASE),
        "Galaxy Z Flip/Fold 1–5",
    ),
    (
        re.compile(r"\b(?:(?:Samsung\s*)?(?:Galaxy\s*)?)?Tab\s*(?:A|S[2-6]e?)\b", re.IGNORECASE),
        "Samsung Galaxy Tab A / S2–S6",
    ),
    (
        re.compile(r"\b(?:(?:Samsung\s*)?(?:Galaxy\s*)?)?A[3-9](?:\s*(?:Plus|\+))?\b(?!\d)", re.IGNORECASE),
        "Galaxy A3–A9 / A-series old",
    ),
    (
        re.compile(r"\b(?:(?:Samsung\s*)?(?:Galaxy\s*)?)?A(?:[1-3][0-4]|5[0-4]|70)(?:\s*(?:Plus|s|\+|5G))?\b(?!\d)", re.IGNORECASE),
        "Galaxy A-series (discontinued A10-A14, A20-A24, A30-A34, A50-A54, A70)",
    ),
    (
        re.compile(r"\b(?:(?:Samsung\s*)?(?:Galaxy\s*)?)?M(?:10|20|30|31|51)(?:\s*(?:Plus|s|\+|5G))?\b(?!\d)", re.IGNORECASE),
        "Galaxy M-series",
    ),
    (
        re.compile(r"\b(?:(?:Samsung\s*)?(?:Galaxy\s*)?)?J[1-9](?!\d)\b", re.IGNORECASE),
        "Galaxy J-series (J1–J8)",
    ),
    (
        re.compile(r"\b(?:SyncMaster\s*(?:B2230|S22B300|S24D300)|Smart\s*Monitor\s*M[57]|All-in-One\s*PC\s*Series\s*[57])\b", re.IGNORECASE),
        "Samsung Legacy Monitor / AIO",
    ),
]

# Dedicated brand-specific patterns for major OEMs when the brand is known
_BRAND_DISCONTINUED_PATTERNS: Dict[str, List[Tuple[re.Pattern, str]]] = {
    "google": [
        (re.compile(r"\b(?:Google\s*)?Pixel\s*(?:[1-8]a?|XL|Fold|[1-8]\s*(?:XL|Pro)?)\b", re.IGNORECASE), "Google Pixel 1–8 / Fold"),
        (re.compile(r"\b(?:Nexus\s*(?:One|S|[4-6]|5X|6P)|Galaxy\s*Nexus)\b", re.IGNORECASE), "Google Nexus"),
        (re.compile(r"\b(?:Pixel\s*Tablet|Pixelbook(?:\s*Go)?|Pixel\s*Slate)\b", re.IGNORECASE), "Google Pixel Tablet/Pixelbook"),
    ],
    "motorola": [
        (re.compile(r"\b(?:Moto\s*G|Moto\s*G\s*)(?:10|[1-9])(?:\s*(?:Plus|Play|Power|Stylus))?\b", re.IGNORECASE), "Motorola Moto G1–G10"),
        (re.compile(r"\b(?:Moto\s*E|Moto\s*E\s*)[4-7](?:\s*(?:Plus|Play|Power))?\b", re.IGNORECASE), "Motorola Moto E4–E7"),
        (re.compile(r"\b(?:Moto\s*Z|Moto\s*Z\s*)[1-4]?(?:\s*(?:Play|Force))?\b", re.IGNORECASE), "Motorola Moto Z series"),
        (re.compile(r"\b(?:Motorola\s*)?Razr(?:\s*\((?:2019)\)|\s*2019|\s*5G|\s*40)?\b", re.IGNORECASE), "Motorola Razr"),
    ],
    "oneplus": [
        (re.compile(r"\b(?:OnePlus\s*)?(?:One|2|3T?|[5-9]T?|10\s*Pro|11)\b", re.IGNORECASE), "OnePlus 1–11 series"),
        (re.compile(r"\b(?:OnePlus\s*)?Nord(?:\s*(?:[1-3]|N10\s*5G|N100|N200|CE(?:\s*[1-3])?))?\b", re.IGNORECASE), "OnePlus Nord series"),
    ],
    "realme": [
        (re.compile(r"\b(?:Realme\s*)?(?:10|[1-9])(?:\s*(?:Pro|Plus|\+|5G|Speed))?\b", re.IGNORECASE), "Realme 1–10 series"),
        (re.compile(r"\b(?:Realme\s*)?C(?:11|15|21|35)\b", re.IGNORECASE), "Realme C series"),
        (re.compile(r"\b(?:Realme\s*)?GT(?:\s*Neo\s*2)?\b", re.IGNORECASE), "Realme GT"),
    ],
    "tecno": [
        (re.compile(r"\b(?:Tecno\s*)?Spark\s*(?:10|[1-9])(?:\s*(?:Pro|Plus|Air|Go|C|Power|Youth))?\b", re.IGNORECASE), "Tecno Spark 1–10"),
        (re.compile(r"\b(?:Tecno\s*)?Camon\s*(?:1[2-9]|20)(?:\s*(?:Premier|Pro|Neo|Plus))?\b", re.IGNORECASE), "Tecno Camon 12–20"),
    ],
    "infinix": [
        (re.compile(r"\b(?:Infinix\s*)?Hot\s*(?:[7-9]|1[0-2])(?:\s*(?:Play|Pro|Lite|i))?\b", re.IGNORECASE), "Infinix Hot 7–12"),
        (re.compile(r"\b(?:Infinix\s*)?Note\s*(?:[7-9]|1[0-2])(?:\s*(?:Pro|VIP|Plus))?\b", re.IGNORECASE), "Infinix Note 7–12"),
    ],
    "itel": [
        (re.compile(r"\b(?:Itel\s*)?(?:A100C|A200|A90|A10|P38)\b", re.IGNORECASE), "Itel A/P series"),
    ],
    "vivo": [
        (re.compile(r"\b(?:Vivo\s*)?V(?:11|15|17|19|2[0135])(?:\s*(?:Pro|e|Lite))?\b", re.IGNORECASE), "Vivo V series"),
        (re.compile(r"\b(?:Vivo\s*)?Y(?:1[1257]|20|30|50)(?:\s*(?:s|i|A))?\b", re.IGNORECASE), "Vivo Y series"),
        (re.compile(r"\b(?:Vivo\s*)?(?:NEX\s*3?|X(?:50|60|70|80)(?:\s*(?:Pro|Pro\+))?)\b", re.IGNORECASE), "Vivo NEX / X series"),
    ],
    "nokia": [
        (re.compile(r"\b(?:Nokia\s*)?Lumia\s*(?:520|625|720|920|1020|1520)\b", re.IGNORECASE), "Nokia Lumia series"),
        (re.compile(r"\b(?:Nokia\s*)?(?:[235678]\.1(?:\s*Plus)?|9\s*PureView|[GX][12]0)\b", re.IGNORECASE), "Nokia Android"),
    ],
    "dell": [
        (re.compile(r"\b(?:Dell\s*)?OptiPlex\s*(?:30[1-7]0|50[4-7]0|70[124-7]0|90[12]0)(?:\s*(?:SFF|MT|Micro|Tower))?\b", re.IGNORECASE), "Dell OptiPlex SFF/MT/Micro"),
        (re.compile(r"\b(?:Dell\s*)?OptiPlex\s*(?:3011|5250|74[4-6]0)\s*AIO\b", re.IGNORECASE), "Dell OptiPlex AIO"),
        (re.compile(r"\b(?:Dell\s*)?Inspiron\s*(?:3250|3650|3847|3470)\s*(?:Desktop|Tower|SFF)?\b", re.IGNORECASE), "Dell Inspiron Desktop"),
        (re.compile(r"\b(?:Dell\s*)?Precision\s*T(?:1650|1700|36[01]0|5600|5810|7810)\b", re.IGNORECASE), "Dell Precision Workstation"),
        (re.compile(r"\b(?:Dell\s*)?Precision\s*(?:35[12]0|75[12]0)\b", re.IGNORECASE), "Dell Precision Laptop"),
        (re.compile(r"\b(?:Dell\s*)?Latitude\s*(?:E?[3-7][0-9x]{3}|[3-7]xxx|3189|3190|3380|3490|3590|5420|5480|5490|5580|5590|7280|7300|7390|7480|7490)\b", re.IGNORECASE), "Dell Latitude"),
        (re.compile(r"\b(?:Dell\s*)?Vostro\s*(?:3400|3500|3590|5471)\b", re.IGNORECASE), "Dell Vostro"),
        (re.compile(r"\b(?:Dell\s*)?(?:UltraSharp\s*U2[247]1[0-5][A-Z]*|P2[0-4]1[49]H|E1913H|E2216H)\b", re.IGNORECASE), "Dell Monitor"),
    ],
    "hp": [
        (re.compile(r"\b(?:HP\s*)?(?:ProDesk\s*[46]00|EliteDesk\s*800)\s*G[1-6](?:\s*(?:SFF|MT|Tower|Mini))?\b", re.IGNORECASE), "HP ProDesk / EliteDesk G1–G6"),
        (re.compile(r"\b(?:HP\s*)?Compaq\s*(?:6000|6200|6300|8200|8300|Desktop|CPU)?(?:\s*(?:Pro|Elite|Desktop|CPU))?\b", re.IGNORECASE), "HP Compaq Pro/Elite"),
        (re.compile(r"\b(?:HP\s*)?(?:ProOne\s*[46]00|EliteOne\s*800)\s*(?:AIO|G[1-6])\b", re.IGNORECASE), "HP ProOne / EliteOne AIO"),
        (re.compile(r"\b(?:HP\s*)?Z(?:2[1-4]0|4[24]0|6[24]0|2\s*Mini\s*G5)\s*(?:Tower|Workstation)?\b", re.IGNORECASE), "HP Z Workstation"),
        (re.compile(r"\b(?:HP\s*)?EliteBook\s*(?:x360\s*)?(?:830\s*G[6-9]|830\s*G10|835\s*G8|840\s*G[7-9]|840\s*G10|845\s*G7|745\s*G6|1040\s*G[2-7]|Folio\s*(?:1020|9[24][78]0m?))\b", re.IGNORECASE), "HP EliteBook"),
        (re.compile(r"\b(?:HP\s*)?ProBook\s*(?:440\s*G[7-9]|450\s*G[7-9]|x360\s*11\s*G[34]\s*EE)\b", re.IGNORECASE), "HP ProBook"),
        (re.compile(r"\b(?:HP\s*)?ZBook\s*(?:14u\s*G5|15\s*G[3-5]|Studio\s*G3)\b", re.IGNORECASE), "HP ZBook"),
        (re.compile(r"\b(?:HP\s*)?(?:250\s*G[5-8]|1[45]-d[aw]\b)", re.IGNORECASE), "HP Essential 250 / 14/15"),
        (re.compile(r"\b(?:HP\s*)?(?:EliteDisplay\s*E2[0-4][1-3]|ProDisplay\s*P2[24]1|ZR2[0-9]{1,3}[a-z]*)\b", re.IGNORECASE), "HP Monitor"),
    ],
    "lenovo": [
        (re.compile(r"\b(?:Lenovo\s*)?ThinkCentre\s*M(?:72e|73|83|92p|93p|700|800|900|710t|720t|910t|920t)(?:\s*(?:SFF|Tower|Tiny))?\b", re.IGNORECASE), "Lenovo ThinkCentre SFF/Tower/Tiny"),
        (re.compile(r"\b(?:Lenovo\s*)?ThinkCentre\s*M(?:72z|92z|93z|800z|900z|910z)(?:\s*AIO)?\b", re.IGNORECASE), "Lenovo ThinkCentre AIO"),
        (re.compile(r"\b(?:Lenovo\s*)?ThinkStation\s*P(?:300|310|320|500|510)\b", re.IGNORECASE), "Lenovo ThinkStation"),
        (re.compile(r"\b(?:Lenovo\s*)?IdeaCentre\s*(?:300|500|[35])(?:\s*Desktop)?\b", re.IGNORECASE), "Lenovo IdeaCentre"),
        (re.compile(r"\b(?:Lenovo\s*)?ThinkPad\s*(?:T4[3-9][05]s?|T495s?|T1[45]\s*Gen\s*[1-3]|X20[01]|X390|X13\s*Gen\s*[12]|P5[0-3]|P14s\s*Gen\s*1|E4[89]0|E14\s*Gen\s*[12]|L3[89]0|L490)\b", re.IGNORECASE), "Lenovo ThinkPad"),
        (re.compile(r"\b(?:Lenovo\s*)?ThinkVision\s*(?:LT(?:1952p|2252p)|T2[234][0-9a-z-]+)\b", re.IGNORECASE), "Lenovo ThinkVision Monitor"),
        (re.compile(r"\b(?:Lenovo\s*)?Tab\s*(?:[234]|M[0-9]+|P1[01])\b", re.IGNORECASE), "Lenovo Tab"),
    ],
    "apple": [
        (re.compile(r"\biMac\s*(?:20|21\.5|24|27)[- ]inch\b", re.IGNORECASE), "Apple iMac"),
        (re.compile(r"\biMac\b.*?\b(?:Retina\s*5K|Unibody|Slim\s*Unibody|Aluminum|200[7-9]|201[0-9]|2020|M1|M3)\b", re.IGNORECASE), "Apple iMac"),
        (re.compile(r"\bMac\s*mini\b.*?\b(?:Intel|201[0-8]|M1|M2)\b", re.IGNORECASE), "Apple Mac mini"),
        (re.compile(r"\bMac\s*Studio\b.*?\bM[12]\b", re.IGNORECASE), "Apple Mac Studio"),
        (re.compile(r"\bMac\s*Pro\b.*?\b(?:Tower|1\.1|5\.1|200[6-9]|201[239])\b", re.IGNORECASE), "Apple Mac Pro"),
        (re.compile(r"\bApple\s*(?:(?:LED\s*)?Cinema\s*Display|Thunderbolt\s*Display)\b", re.IGNORECASE), "Apple Cinema / Thunderbolt Display"),
        (re.compile(r"\biPhone(?:\s*(?:Base\b|3G[S]?|4S?|5[sc]?|6\s*Plus|6s(?:\s*Plus)?|7\s*Plus|[3-8]|X[RS]?|1[1-4]))(?:\s*(?:Pro|Max|Pro\s*Max|Plus|Mini|Base|s|c))?\b(?!\s*1[5-7])", re.IGNORECASE), "Apple iPhone 3–14"),
        (re.compile(r"\biPhone\s*SE\s*(?:[1-3](?:st|nd|rd)?\s*gen|(?:2016|2020|2022))\b", re.IGNORECASE), "Apple iPhone SE"),
        (re.compile(r"\biPad\s*(?:Mini|Air|Pro)?\b", re.IGNORECASE), "Apple iPad"),
    ],
    "asus": [
        (re.compile(r"\bAsus\s*(?:VivoBook\b.*?\b201[5-8]\b|VivoBook\s*201[5-8]|VivoPC\s*VM[46]0[A-Z]?|ROG\s*G(?:R8|20)|VS2[24][0-9]H|VP228H)\b", re.IGNORECASE), "Asus Desktop / Monitor / Laptop"),
    ],
    "acer": [
        (re.compile(r"\bAcer\s*(?:Veriton\s*[XMN][24]630G|Aspire\s*C2[24]\s*AIO|V196HQL|K222HQL|Aspire\s*[357])\b", re.IGNORECASE), "Acer Desktop / Monitor"),
    ],
    "oppo": [
        (re.compile(r"\bOppo\s*(?:A(?:3[01]?|5s?|9|1[2456]|[5-7][04])|Find\s*X[0-3]?|Reno(?:\s*[1-5])?)\b", re.IGNORECASE), "Oppo A/Find/Reno"),
    ],
    "htc": [
        (re.compile(r"\b(?:HTC\s*)?(?:One\s*M[789]|10|U11|U12\+?)\b", re.IGNORECASE), "HTC One / U series"),
    ],
    "blackberry": [
        (re.compile(r"\b(?:BlackBerry\s*)?(?:Bold\s*9[09]00|Curve\s*8520|Z10|Q10|Priv|Key[Oo]ne|Key2)\b", re.IGNORECASE), "BlackBerry legacy phone"),
    ],
    "microsoft": [
        (re.compile(r"\bSurface\s*(?:Laptop\s*[1-5]|Book\s*[1-3]|Go\s*[1-3])\b", re.IGNORECASE), "Microsoft Surface Laptop/Book/Go"),
    ],
    "toshiba": [
        (re.compile(r"\b(?:Toshiba\s*)?(?:Tecra\s*A50|Port[eé]g[eé]\s*Z30|Dynabook\s*B65)\b", re.IGNORECASE), "Toshiba Tecra/Portege/Dynabook"),
    ],
    "fujitsu": [
        (re.compile(r"\b(?:Fujitsu\s*)?Life[bB]ook\s*(?:U748|E5410)\b", re.IGNORECASE), "Fujitsu LifeBook"),
    ],
    "panasonic": [
        (re.compile(r"\b(?:Panasonic\s*)?Toughbook\s*CF[- ]?[1-5][0-9]?\b", re.IGNORECASE), "Panasonic Toughbook"),
    ],
}


def _match_discontinued_model(title: str, brand: str = "") -> Optional[str]:
    """Check if title matches any model in the Discontinued Models master list."""
    if not title:
        return None

    brand_lower = (brand or "").lower().strip()
    title_lower = title.lower()

    # 1. Samsung brand-aware check (catches 'Note 10', 'Galaxy 23 ultra', 'S10', 'Z Fold 4', etc.)
    is_samsung = (
        brand_lower == "samsung"
        or "samsung" in brand_lower
        or "samsung" in title_lower
        or "galaxy" in title_lower
    )
    if is_samsung:
        for pattern, label in _SAMSUNG_DISCONTINUED_PATTERNS:
            if pattern.search(title):
                return label

    # 2. Check brand-specific patterns if brand matches a known OEM
    for oem, patterns in _BRAND_DISCONTINUED_PATTERNS.items():
        if oem in brand_lower or oem in title_lower:
            for pattern, label in patterns:
                if pattern.search(title):
                    return label

    # 3. Check general discontinued models list (for generic brands or when prefix is present)
    for _, pattern, label in DISCONTINUED_MODELS_CONFIG:
        if pattern.search(title):
            # Guard gaming consoles against matching game discs, controllers, and accessories
            if ("Console" in label or "Handheld" in label) and GAME_SOFTWARE_ACCESSORY_GUARD_RE.search(title):
                continue
            return label

    return None


def _is_refurb_eligible_category(cat_str: str, cat_code: str = "") -> bool:
    """Return True if category is eligible for refurbished listings.

    Priority order:
      1. Exact category-code match against REFURB_ELIGIBLE_CATEGORY_CODES (most precise).
      2. Path-string keyword match for phones/tablets (not covered by the code list).
      3. Ineligible-branch exclusion for everything else.
    """
    # 1. Code whitelist — fastest, most precise
    if cat_code and cat_code.strip():
        if cat_code.strip() in REFURB_ELIGIBLE_CATEGORY_CODES:
            return True
        if not cat_str:
            return False  # known non-eligible category code

    if not cat_str:
        return True  # completely unknown category → fall back to title-level INELIGIBLE_REFURB_PRODUCTS_RE
    c = cat_str.lower()

    # 2. Check ineligible branches FIRST to prevent false positives like headphones, stationery notebooks, etc.
    ineligible_branches = (
        "game", "gaming", "console", "playstation", "xbox", "nintendo",
        "printer", "scanner", "copier",
        "tv", "television", "home audio", "home theatre", "audio", "speaker", "headphone", "earphone", "earbud", "earbuds",
        "camera", "optics", "photo",
        "appliance", "kitchen", "fashion", "clothing", "shoes", "beauty", "health",
        "watch", "wearable", "smartwatch",
        "paper", "stationery", "writing pads", "office & school supplies",
        "book", "books", "novel", "literature", "media", "magazine",
        "phone accessories", "laptop accessories", "tablet accessories",
        "bag", "case", "cover", "holder",
    )
    if any(b in c for b in ineligible_branches):
        return False

    # 3. Laptops / desktops / phones / tablets / monitors with precise word boundaries
    is_laptop_or_desktop = any(
        re.search(r"\b" + re.escape(w) + r"\b", c)
        for w in (
            "laptop", "laptops", "notebook", "notebooks", "netbook", "macbook",
            "ultrabook", "chromebook", "desktop", "desktops", "all-in-one", "tower"
        )
    )
    # Ensure 'notebook' only matches under computing/electronics, not paper/stationery
    if "notebook" in c and not any(comp in c for comp in ("computer", "computing", "pc", "electronics")):
        is_laptop_or_desktop = False

    is_phone = bool(re.search(r"\b(?:smartphones?|mobile\s*phones?|cell\s*phones?|mobile)\b", c)) or (
        bool(re.search(r"\bphones?\b", c)) and not any(sub in c for sub in ("headphone", "earphone", "microphone"))
    )
    is_tablet = bool(re.search(r"\b(?:tablets?|ipads?)\b", c))
    is_monitor = bool(re.search(r"\bmonitors?\b", c))

    if is_laptop_or_desktop or is_phone or is_tablet or is_monitor:
        return True

    return False  # anything else not explicitly eligible is out of scope



def check_refurbished_products(
    data: pd.DataFrame,
    code_to_path: Optional[Dict[str, str]] = None,
    **kwargs,
) -> pd.DataFrame:
    """
    Unified validation for refurbished products implementing rules REF-01 through REF-11.
    Evaluates:
      1. REF-04: Refurb claims outside Laptops/Phones/Tablets (PlayStations, printers, TVs, etc.)
      2. REF-11: Already-Compliant Suppression Guard (Laptops, Phones, Tablets)
      3. REF-07: Component / Accessory Guard
      4. REF-06: Discontinued device marked 'Brand New' / 'Sealed'
      5. REF-10: Seller Name as Brand
      6. REF-09: Generic / Unmapped / Blank Brand on Refurb Item
      7. REF-01 / REF-02 / REF-05: OEM Brand & Misspelled Terms & Discontinued Models
      8. REF-03: Brand = Renewed/Refurbished but Title Missing 'Refurbished'
    """
    if data.empty or "NAME" not in data.columns:
        return pd.DataFrame(columns=data.columns)

    code_to_path = code_to_path or {}
    flagged_rows = []

    for idx, row in data.iterrows():
        title = str(row.get("NAME") or "").strip()
        brand_raw = str(row.get("BRAND") or "").strip()
        brand_lower = brand_raw.lower()
        seller_raw = str(row.get("SELLER_NAME") or "").strip()
        seller_lower = seller_raw.lower()

        # Category resolution
        cat_code = str(row.get("CATEGORY_CODE") or "").strip()
        cat_path = ""
        if "CATEGORY" in row and pd.notna(row["CATEGORY"]) and str(row["CATEGORY"]).strip():
            cat_path = str(row["CATEGORY"]).strip()
        elif cat_code in code_to_path:
            cat_path = code_to_path[cat_code]
        elif "_cat_clean" in row and str(row["_cat_clean"]) in code_to_path:
            cat_path = code_to_path[str(row["_cat_clean"])]

        # Determine category eligibility and device eligibility
        # Pass cat_code so the whitelist is checked first (fastest + most precise path)
        is_eligible_category = _is_refurb_eligible_category(cat_path, cat_code=cat_code)
        is_ineligible_device = bool(INELIGIBLE_REFURB_PRODUCTS_RE.search(title))
        is_outside_scope = (not is_eligible_category) or is_ineligible_device

        # Clean title for condition terms scanning:
        # Strip grammatical verb usages of 'used' ('used as', 'used for', 'can be used', etc.)
        title_for_refurb_scan = VERB_USED_RE.sub(" ", title)

        # Title keyword checks
        has_canonical_term = bool(
            re.search(r"\b(refurbished|renewed)\b", title_for_refurb_scan, re.IGNORECASE)
        )
        has_nonstandard_match = NONSTANDARD_REFURB_RE.search(title_for_refurb_scan)
        has_any_refurb_match = ALL_REFURB_INDICATORS_RE.search(title_for_refurb_scan)
        refurb_term_found = (
            has_any_refurb_match.group(0) if has_any_refurb_match else ""
        )

        # Brand check
        brand_is_refurb = brand_lower in APPROVED_REFURB_BRANDS
        brand_is_oem = brand_lower in OEM_BRANDS
        brand_is_generic_or_blank = (
            brand_lower in GENERIC_BRANDS or not brand_lower
        )
        brand_matches_seller = (
            bool(seller_lower)
            and len(seller_lower) > 2
            and brand_lower == seller_lower
            and not brand_is_refurb
        )

        # Refurb scanning is only applicable for tech / home & living categories.
        # For beauty, fashion, groceries, etc. we skip ALL refurb rule processing
        # entirely — this prevents false positives on products like lotions or
        # cleaning supplies that may legitimately use words like 'renewed'.
        _cp = cat_path.lower()
        is_tech_dept = (
            is_eligible_category  # already whitelisted by code/keyword
            or any(kw in _cp for kw in (
                "computing", "computer", "laptop", "phone", "tablet", "mobile",
                "electronics", "electronic", "monitor", "desktop",
                "home & living", "home and living", "appliance",
            ))
        )
        if not is_tech_dept and not is_ineligible_device and not brand_is_refurb:
            # Outside tech scope: skip this row entirely
            continue

        # Discontinued model check (with REF-08 guard)
        discontinued_model = _match_discontinued_model(title, brand=brand_raw)
        is_discontinued = discontinued_model is not None

        # ------------------------------------------------------------------
        # RULE REF-04: Refurb Terms Outside Laptops / Phones / Tablets
        # Refurbished listings are strictly permitted for Laptops, Phones, and Tablets only.
        # Any refurb claim (in title or brand) on PlayStations, printers, TVs, monitors, etc.
        # must be rejected outright.
        # ------------------------------------------------------------------
        if is_outside_scope and (has_any_refurb_match or brand_is_refurb):
            r = row.copy()
            r["Reason"] = REASON_OTHER_REASON
            display_cat = cat_path or cat_code or "Ineligible Category"
            claim = f"term '{refurb_term_found}'" if refurb_term_found else f"brand '{brand_raw}'"
            r["Comment_Detail"] = (
                f"Refurbished/Renewed listings are strictly permitted for Laptops, Phones, and Tablets only. "
                f"Refurbished claim ({claim}) is not permitted for this product type / category '{display_cat}'."
            )
            flagged_rows.append(r)
            continue

        # If outside category scope and has no refurb terms, nothing further to check
        if is_outside_scope:
            continue

        # ------------------------------------------------------------------
        # GUARD REF-11: Already-Compliant Suppression Guard (Laptops, Phones, Tablets)
        # If title has 'Refurbished'/'Renewed' AND brand is Refurbished/Renewed,
        # SKU is fully compliant!
        # ------------------------------------------------------------------
        if has_canonical_term and brand_is_refurb:
            continue

        # ------------------------------------------------------------------
        # GUARD REF-07: Component / Accessory Guard
        # Bare parts (screen only, battery only, etc.) are excluded from refurb-brand rules
        # ------------------------------------------------------------------
        is_accessory_or_part = bool(COMPONENT_ACCESSORY_GUARD_RE.search(title))
        if is_accessory_or_part:
            continue

        # ------------------------------------------------------------------
        # RULE REF-06: Discontinued Device Marked 'Brand New' / 'Sealed'
        # ------------------------------------------------------------------
        if is_discontinued:
            brand_new_match = BRAND_NEW_CLAIM_RE.search(title)
            if brand_new_match:
                claim_term = brand_new_match.group(0)
                r = row.copy()
                r["Reason"] = REASON_SELLER_SUPPORT
                r["Comment_Detail"] = (
                    f"Discontinued model '{discontinued_model}' is claimed as '{claim_term}' "
                    f"(Brand New/Sealed). Direct contradiction for out-of-market hardware; "
                    f"listing rejected. Proof of new stock or refurb reclassification required."
                )
                flagged_rows.append(r)
                continue

        # ==================================================================
        # MANDATORY 3-STEP REFURB VALIDATION
        # Any refurb trigger (keyword, sourcing tag, discontinued model)
        # must satisfy ALL three gates in order:
        #   STEP 1 — Brand must be 'Refurbished' or 'Renewed'
        #   STEP 2 — Title must contain the word 'Refurbished'
        #   STEP 3 — Seller must be authorised to list refurbished items
        # ==================================================================
        is_refurb_item = bool(has_any_refurb_match or is_discontinued)
        has_typo = bool(has_nonstandard_match and not has_canonical_term)
        typo_str = has_nonstandard_match.group(0) if has_nonstandard_match else ""

        # ------------------------------------------------------------------
        # STEP 1: Brand must be 'Refurbished' or 'Renewed'
        # ------------------------------------------------------------------
        if is_refurb_item and not brand_is_refurb:
            r = row.copy()
            r["Reason"] = REASON_BRAND_NOT_ALLOWED

            if brand_matches_seller:
                r["Comment_Detail"] = (
                    f"Brand field matches seller storefront name ('{seller_raw}'). "
                    f"Refurbished/Renewed items must have brand set to 'Refurbished' or 'Renewed'. "
                    f"Title must also contain the word 'Refurbished'."
                )
            elif brand_is_generic_or_blank:
                trigger = f"refurb term '{refurb_term_found}'" if refurb_term_found else f"discontinued model '{discontinued_model}'"
                r["Comment_Detail"] = (
                    f"Product indicates refurbished status ({trigger}), but brand is "
                    f"'{brand_raw or 'BLANK'}'. Brand must be set to 'Refurbished' or 'Renewed' "
                    f"and the title must contain the word 'Refurbished'."
                )
            else:
                details = []
                if is_discontinued:
                    details.append(f"discontinued model '{discontinued_model}'")
                if has_typo:
                    details.append(f"non-standard term '{typo_str}'")
                elif refurb_term_found:
                    details.append(f"refurb indicator '{refurb_term_found}'")
                detail_str = f" ({', '.join(details)})" if details else ""
                r["Comment_Detail"] = (
                    f"OEM brand '{brand_raw}' is not permitted on a refurbished listing{detail_str}. "
                    f"Brand must be set to 'Refurbished' or 'Renewed' and the title must contain "
                    f"the word 'Refurbished'. e.g. 'Renewed {brand_raw} ...' with brand field = 'Renewed'."
                )
            flagged_rows.append(r)
            continue

        # ------------------------------------------------------------------
        # STEP 2: Title must contain 'Refurbished' (or 'Renewed')
        # Brand is already confirmed Refurbished/Renewed at this point.
        # ------------------------------------------------------------------
        if is_refurb_item and not has_canonical_term:
            r = row.copy()
            r["Reason"] = REASON_IMPROVE_NAME
            if has_typo:
                r["Comment_Detail"] = (
                    f"Product name contains non-standard refurb term ('{typo_str}'). "
                    f"Replace with the standard word 'Refurbished' in the title."
                )
            else:
                trigger = f"refurb indicator '{refurb_term_found}'" if refurb_term_found else f"discontinued model '{discontinued_model}'"
                r["Comment_Detail"] = (
                    f"Product is identified as refurbished ({trigger}) but the word "
                    f"'Refurbished' is missing from the title. Please add 'Refurbished' "
                    f"to the product name (e.g. 'Renewed Brand Model (Refurbished)')."
                )
            flagged_rows.append(r)
            continue

        # ------------------------------------------------------------------
        # STEP 3: Seller must be authorised to list refurbished items
        # Both brand and title have passed at this point.
        # ------------------------------------------------------------------
        if is_refurb_item and seller_raw:
            _cc = cat_code.strip()
            _cp2 = cat_path.lower()
            _is_phone = _cc in {"1002209","1002221","1002235","1002252","1002282","1002300","1002314","1002333","1002345"} \
                or any(w in _cp2 for w in ("phone", "smartphone", "mobile"))
            _is_laptop = any(w in _cp2 for w in ("laptop", "notebook", "macbook", "ultrabook", "chromebook")) \
                or _cc in {"1003787","1003803","1029459","1003815","1003831","1003845","1029467","1003860"}
            _sellers = {}
            _approved_phones: set = set()
            _approved_laptops: set = set()
            # seller data may be injected via kwargs
            if hasattr(check_refurbished_products, "_refurb_data"):
                _sd = getattr(check_refurbished_products, "_refurb_data", {})
                _sellers = _sd.get("sellers", {}).get(getattr(check_refurbished_products, "_country_code", ""), {})
                _approved_phones = _sellers.get("Phones", set())
                _approved_laptops = _sellers.get("Laptops", set())

            if _approved_phones and _is_phone and seller_lower not in _approved_phones:
                r = row.copy()
                r["Reason"] = REASON_SELLER_SUPPORT
                r["Comment_Detail"] = (
                    f"Seller '{seller_raw}' is not authorised to list refurbished Phones. "
                    f"Contact Seller Support for Refurbished approval."
                )
                flagged_rows.append(r)
                continue
            if _approved_laptops and _is_laptop and seller_lower not in _approved_laptops:
                r = row.copy()
                r["Reason"] = REASON_SELLER_SUPPORT
                r["Comment_Detail"] = (
                    f"Seller '{seller_raw}' is not authorised to list refurbished Laptops. "
                    f"Contact Seller Support for Refurbished approval."
                )
                flagged_rows.append(r)
                continue

    if not flagged_rows:
        return pd.DataFrame(columns=data.columns)

    out_df = pd.DataFrame(flagged_rows)
    if "PRODUCT_SET_SID" in out_df.columns:
        out_df = out_df.drop_duplicates(subset=["PRODUCT_SET_SID"])
    return out_df


def check_out_of_market_devices(
    data: pd.DataFrame,
    code_to_path: Optional[Dict[str, str]] = None,
    **kwargs,
) -> pd.DataFrame:
    """
    Dedicated validation for Out of Market / Discontinued devices (REF-05 / REF-06).
    Flags hardware from out_of_market_devicesvv.xlsx (laptops, phones, tablets)
    listed under OEM brand, generic brand, or claimed as 'Brand New' / 'Sealed'.
    Separated from general refurbished rules for reporting visibility.
    """
    if data.empty or "NAME" not in data.columns:
        return pd.DataFrame(columns=data.columns)

    code_to_path = code_to_path or {}
    flagged_rows = []

    for idx, row in data.iterrows():
        title = str(row.get("NAME") or "").strip()
        brand_raw = str(row.get("BRAND") or "").strip()
        brand_lower = brand_raw.lower()
        seller_raw = str(row.get("SELLER_NAME") or "").strip()
        seller_lower = seller_raw.lower()

        # Category resolution
        cat_code = str(row.get("CATEGORY_CODE") or "").strip()
        cat_path = ""
        if "CATEGORY" in row and pd.notna(row["CATEGORY"]) and str(row["CATEGORY"]).strip():
            cat_path = str(row["CATEGORY"]).strip()
        elif cat_code in code_to_path:
            cat_path = code_to_path[cat_code]
        elif "_cat_clean" in row and str(row["_cat_clean"]) in code_to_path:
            cat_path = code_to_path[str(row["_cat_clean"])]

        is_eligible_category = _is_refurb_eligible_category(cat_path, cat_code=cat_code)
        is_ineligible_device = bool(INELIGIBLE_REFURB_PRODUCTS_RE.search(title))
        if not is_eligible_category or is_ineligible_device:
            continue

        # Accessory or bare part guard (REF-07)
        if bool(COMPONENT_ACCESSORY_GUARD_RE.search(title)):
            continue

        # Check if model is out-of-market / discontinued
        discontinued_model = _match_discontinued_model(title, brand=brand_raw)
        if not discontinued_model:
            continue

        title_for_refurb_scan = VERB_USED_RE.sub(" ", title)
        has_canonical_term = bool(
            re.search(r"\b(refurbished|renewed)\b", title_for_refurb_scan, re.IGNORECASE)
        )
        brand_is_refurb = brand_lower in APPROVED_REFURB_BRANDS

        # Already-compliant guard (REF-11): Title has 'Refurbished'/'Renewed' AND brand is Refurbished/Renewed
        if has_canonical_term and brand_is_refurb:
            continue

        # REF-06: Discontinued device marked 'Brand New' / 'Sealed'
        brand_new_match = BRAND_NEW_CLAIM_RE.search(title)
        if brand_new_match:
            claim_term = brand_new_match.group(0)
            r = row.copy()
            r["FLAG"] = "Out of market devices"
            r["Reason"] = REASON_SELLER_SUPPORT
            r["Comment_Detail"] = (
                f"Discontinued out-of-market model '{discontinued_model}' is claimed as '{claim_term}' "
                f"(Brand New/Sealed). Direct contradiction for out-of-market hardware; "
                f"listing rejected. Proof of new stock or refurb reclassification required."
            )
            flagged_rows.append(r)
            continue

        # REF-05: Listed under OEM or generic brand without refurb classification
        if not brand_is_refurb:
            r = row.copy()
            r["FLAG"] = "Out of market devices"
            r["Reason"] = REASON_BRAND_NOT_ALLOWED
            r["Comment_Detail"] = (
                f"Out-of-market / discontinued model '{discontinued_model}' cannot be listed as "
                f"brand new under OEM brand '{brand_raw}'. Brand must be set to 'Refurbished' "
                f"or 'Renewed' and the title must contain the word 'Refurbished'."
            )
            flagged_rows.append(r)
            continue

        # Brand is Refurbished, but title is missing 'Refurbished'
        if not has_canonical_term:
            r = row.copy()
            r["FLAG"] = "Out of market devices"
            r["Reason"] = REASON_IMPROVE_NAME
            r["Comment_Detail"] = (
                f"Out-of-market device '{discontinued_model}' has brand '{brand_raw}', but title "
                f"must state 'Refurbished' or 'Renewed' (e.g. '{brand_raw} {discontinued_model} (Refurbished)')."
            )
            flagged_rows.append(r)
            continue

    if not flagged_rows:
        return pd.DataFrame(columns=data.columns)

    out_df = pd.DataFrame(flagged_rows)
    if "PRODUCT_SET_SID" in out_df.columns:
        out_df = out_df.drop_duplicates(subset=["PRODUCT_SET_SID"])
    return out_df


def check_refurb_seller_approval(
    data: pd.DataFrame, refurb_data: dict, country_code: str, code_to_path: Optional[Dict] = None
) -> pd.DataFrame:
    required = {"PRODUCT_SET_SID", "CATEGORY_CODE", "SELLER_NAME", "NAME"}
    if not required.issubset(data.columns):
        return pd.DataFrame(columns=data.columns)
    phone_cats = refurb_data.get("categories", {}).get("Phones", set())
    laptop_cats = refurb_data.get("categories", {}).get("Laptops", set())
    keywords = refurb_data.get("keywords", set())
    sellers = refurb_data.get("sellers", {}).get(country_code, {})
    if not phone_cats and not laptop_cats and not code_to_path:
        return pd.DataFrame(columns=data.columns)

    kw_pattern = None
    if keywords:
        kw_pattern = re.compile(
            r"\b(?:"
            + "|".join(re.escape(k) for k in sorted(keywords, key=len, reverse=True))
            + r")\b",
            re.IGNORECASE,
        )

    d = data
    c2p = code_to_path or {}

    _cat_clean = d["_cat_clean"] if "_cat_clean" in d.columns else d["CATEGORY_CODE"].astype(str).str.strip()
    cat_paths = _cat_clean.map(c2p).fillna("").astype(str).str.lower()
    is_phone = _cat_clean.isin(phone_cats) | cat_paths.str.contains(r"\b(?:phone|smartphone|mobile)\b", regex=True)
    is_laptop = _cat_clean.isin(laptop_cats) | cat_paths.str.contains(r"\b(?:laptop|notebook|macbook|ultrabook)\b", regex=True)
    in_scope = is_phone | is_laptop

    _brand_lower = d["_brand_lower"] if "_brand_lower" in d.columns else d["BRAND"].fillna("").astype(str).str.strip().str.lower()
    is_refurb_brand = _brand_lower.isin(APPROVED_REFURB_BRANDS)
    has_keyword = pd.Series(False, index=d.index)
    if kw_pattern:
        has_keyword = d["NAME"].astype(str).str.contains(kw_pattern, na=False)
    is_refurb = is_refurb_brand | has_keyword

    approved_phones = sellers.get("Phones", set())
    approved_laptops = sellers.get("Laptops", set())
    _seller_lower = d["_seller_lower"] if "_seller_lower" in d.columns else d["SELLER_NAME"].fillna("").astype(str).str.strip().str.lower()
    not_approved = (is_phone & ~_seller_lower.isin(approved_phones)) | (
        is_laptop & ~_seller_lower.isin(approved_laptops)
    )

    flagged = d[in_scope & is_refurb & not_approved].copy()
    if not flagged.empty:
        REASON_UNAPPROVED_REFURB_SELLER = (
            "1000028 - Kindly Contact Jumia Seller Support To Confirm Possibility Of Sale Of This Product By Raising A Claim"
        )
        flagged["Reason"] = REASON_UNAPPROVED_REFURB_SELLER

        def build_comment(row):
            cc = str(row.get("CATEGORY_CODE", "")).strip()
            ptype = "Phone" if cc in phone_cats or "phone" in str(c2p.get(cc, "")).lower() else "Laptop"
            seller = row.get("SELLER_NAME", "")
            return f"Unapproved {ptype} refurb seller — seller '{seller}' is not authorized to sell refurbished {ptype}s in {country_code}."

        flagged["Comment_Detail"] = flagged.apply(build_comment, axis=1)
    return flagged.drop_duplicates(subset=["PRODUCT_SET_SID"])


def audit_refurbished_record(
    rec: Dict,
    code_to_path: Optional[Dict[str, str]] = None,
    refurb_data: Optional[Dict] = None,
    country_code: str = "",
) -> Dict:
    """Evaluates a single product record against refurbished rules for the targeted audit.

    Returns a dict with:
      - is_refurb: bool (whether the product has any refurbished signal/claim/discontinued model)
      - is_compliant: bool (whether it conforms fully to REF-11 and seller authorization)
      - violation: Optional[Dict] with 'reason_type', 'reason_code', 'detail'
    """
    code_to_path = code_to_path or {}
    refurb_data = refurb_data or {}

    title = str(rec.get("NAME") or "").strip()
    brand_raw = str(rec.get("BRAND") or "").strip()
    brand_lower = brand_raw.lower()
    seller_raw = str(rec.get("SELLER_NAME") or "").strip()
    seller_lower = seller_raw.lower()

    cat_code = str(rec.get("CATEGORY_CODE") or "").strip()
    cat_path = ""
    if "CATEGORY" in rec and pd.notna(rec.get("CATEGORY")) and str(rec.get("CATEGORY")).strip():
        cat_path = str(rec.get("CATEGORY")).strip()
    elif cat_code in code_to_path:
        cat_path = code_to_path[cat_code]
    elif "_cat_clean" in rec and str(rec.get("_cat_clean")) in code_to_path:
        cat_path = code_to_path[str(rec.get("_cat_clean"))]

    is_eligible_category = _is_refurb_eligible_category(cat_path, cat_code=cat_code)
    is_ineligible_device = bool(INELIGIBLE_REFURB_PRODUCTS_RE.search(title))
    is_outside_scope = (not is_eligible_category) or is_ineligible_device

    # Clean title for condition terms scanning
    title_for_refurb_scan = VERB_USED_RE.sub(" ", title)

    has_canonical_term = bool(
        re.search(r"\b(refurbished|renewed)\b", title_for_refurb_scan, re.IGNORECASE)
    )
    has_nonstandard_match = NONSTANDARD_REFURB_RE.search(title_for_refurb_scan)
    has_any_refurb_match = ALL_REFURB_INDICATORS_RE.search(title_for_refurb_scan)
    refurb_term_found = has_any_refurb_match.group(0) if has_any_refurb_match else ""

    brand_is_refurb = brand_lower in APPROVED_REFURB_BRANDS
    brand_is_generic_or_blank = brand_lower in GENERIC_BRANDS or not brand_lower
    brand_matches_seller = (
        bool(seller_lower) and len(seller_lower) > 2 and brand_lower == seller_lower and not brand_is_refurb
    )

    # Tech-dept allowlist guard (mirrors main loop logic)
    _cp_a = cat_path.lower()
    _is_tech_dept = (
        is_eligible_category
        or any(kw in _cp_a for kw in (
            "computing", "computer", "laptop", "phone", "tablet", "mobile",
            "electronics", "electronic", "monitor", "desktop",
            "home & living", "home and living", "appliance",
        ))
    )
    if not _is_tech_dept and not is_ineligible_device and not brand_is_refurb:
        return {"is_refurb": False, "is_compliant": False, "violation": None}

    discontinued_model = _match_discontinued_model(title, brand=brand_raw)
    is_discontinued = discontinued_model is not None

    is_refurb_item = bool(has_any_refurb_match or brand_is_refurb or is_discontinued)
    if not is_refurb_item:
        return {"is_refurb": False, "is_compliant": False, "violation": None}

    # Accessory or part guard
    is_accessory_or_part = bool(COMPONENT_ACCESSORY_GUARD_RE.search(title))
    if is_accessory_or_part:
        return {"is_refurb": True, "is_compliant": True, "violation": None}

    # 1. REF-04: Refurb terms outside Laptops/Phones/Tablets
    if is_outside_scope and (has_any_refurb_match or brand_is_refurb):
        display_cat = cat_path or cat_code or "Ineligible Category"
        claim = f"term '{refurb_term_found}'" if refurb_term_found else f"brand '{brand_raw}'"
        return {
            "is_refurb": True,
            "is_compliant": False,
            "violation": {
                "reason_type": "AI Missed Refurbished - Ineligible Category / Device",
                "reason_code": REASON_OTHER_REASON,
                "detail": (
                    f"Refurbished/Renewed listings are strictly permitted for Laptops, Phones, and Tablets only. "
                    f"Refurbished claim ({claim}) is not permitted for product type / category '{display_cat}'."
                ),
            },
        }

    if is_outside_scope:
        return {"is_refurb": False, "is_compliant": False, "violation": None}

    # 2. REF-06: Discontinued device claimed 'Brand New' / 'Sealed'
    if is_discontinued:
        brand_new_match = BRAND_NEW_CLAIM_RE.search(title)
        if brand_new_match:
            claim_term = brand_new_match.group(0)
            return {
                "is_refurb": True,
                "is_compliant": False,
                "violation": {
                    "reason_type": "AI Missed Refurbished - Discontinued Device Claimed New",
                    "reason_code": REASON_SELLER_SUPPORT,
                    "detail": (
                        f"Discontinued model '{discontinued_model}' is claimed as '{claim_term}' "
                        f"(Brand New/Sealed). Direct contradiction for out-of-market hardware; "
                        f"listing rejected. Proof of new stock or refurb reclassification required."
                    ),
                },
            }

    # ------------------------------------------------------------------
    # STEP 1 (audit): Brand must be 'Refurbished' or 'Renewed'
    # ------------------------------------------------------------------
    # (falls through to step 2 if brand already passes)

    # ------------------------------------------------------------------
    # STEP 2 (audit): Title must contain 'Refurbished'
    # ------------------------------------------------------------------
    # (checked below after brand gate)

    # ------------------------------------------------------------------
    # STEP 3 (audit): Seller must be authorised
    # Only reached when brand=OK and title=OK
    # ------------------------------------------------------------------

    # 4. Mandatory Refurb Brand Enforcement
    has_typo = bool(has_nonstandard_match and not has_canonical_term)
    typo_str = has_nonstandard_match.group(0) if has_nonstandard_match else ""

    if not brand_is_refurb:
        if brand_matches_seller:
            # Seller name used as brand on a refurb item
            return {
                "is_refurb": True,
                "is_compliant": False,
                "violation": {
                    "reason_type": "AI Missed Refurbished - Seller Name as Brand",
                    "reason_code": REASON_BRAND_NOT_ALLOWED,
                    "detail": (
                        f"Brand field matches seller storefront name ('{seller_raw}'). "
                        f"Refurbished/Renewed items must have brand set to 'Refurbished' or 'Renewed'."
                    ),
                },
            }
        elif brand_is_generic_or_blank:
            # REF-09: Generic/blank brand on a refurb item — genuine violation, not an overturn
            trigger = f"refurb term '{refurb_term_found}'" if refurb_term_found else f"discontinued model '{discontinued_model}'"
            return {
                "is_refurb": True,
                "is_compliant": False,
                "violation": {
                    "reason_type": "AI Missed Refurbished - Generic/Blank Brand on Refurb Item",
                    "reason_code": REASON_BRAND_NOT_ALLOWED,
                    "detail": (
                        f"Product indicates refurbished status ({trigger}), but brand is "
                        f"'{brand_raw or 'BLANK'}'. Refurbished items must have brand set to 'Refurbished' or 'Renewed'."
                    ),
                },
            }
        else:
            # REF-01: OEM brand (e.g. HP, Dell, Samsung) is not permitted on a refurb item
            details = []
            if is_discontinued:
                details.append(f"discontinued model '{discontinued_model}'")
            if has_typo:
                details.append(f"misspelled/non-standard term '{typo_str}'")
            elif refurb_term_found:
                details.append(f"refurb indicator '{refurb_term_found}'")
            detail_str = f" ({', '.join(details)})" if details else ""
            fix_note = (
                " and product title updated to include standard 'Refurbished'."
                if (has_typo or not has_canonical_term)
                else "."
            )
            return {
                "is_refurb": True,
                "is_compliant": False,
                "violation": {
                    "reason_type": "AI Missed Refurbished - OEM Brand Instead of Refurb Brand",
                    "reason_code": REASON_BRAND_NOT_ALLOWED,
                    "detail": (
                        f"OEM brand '{brand_raw}' is not permitted on a refurbished listing{detail_str}. "
                        f"Brand must be set to 'Refurbished' or 'Renewed'{fix_note} "
                        f"The product name should read e.g. 'Renewed {brand_raw} ...' with brand field set to 'Renewed'."
                    ),
                },
            }

    # 5. Title check — must contain 'Refurbished' (STEP 2 audit gate)
    if has_typo:
        return {
            "is_refurb": True,
            "is_compliant": False,
            "violation": {
                "reason_type": "AI Missed Refurbished - Misspelled / Non-Standard Term in Title",
                "reason_code": REASON_IMPROVE_NAME,
                "detail": (
                    f"Product name contains non-standard or misspelled refurb terminology ('{typo_str}'). "
                    f"Replace with the standard word 'Refurbished' in the title."
                ),
            },
        }

    if not has_canonical_term:
        trigger = f"refurb indicator '{refurb_term_found}'" if refurb_term_found else f"discontinued model '{discontinued_model}'"
        return {
            "is_refurb": True,
            "is_compliant": False,
            "violation": {
                "reason_type": "AI Missed Refurbished - Missing 'Refurbished' in Title",
                "reason_code": REASON_IMPROVE_NAME,
                "detail": (
                    f"Product is identified as refurbished ({trigger}) but the word 'Refurbished' "
                    f"is missing from the title. Please add 'Refurbished' to the product name."
                ),
            },
        }

    # 6. Seller authorisation check (STEP 3 audit gate)
    # Brand=OK, Title=OK — now verify the seller is approved
    if refurb_data and seller_raw:
        _phone_cats = refurb_data.get("categories", {}).get("Phones", set())
        _laptop_cats = refurb_data.get("categories", {}).get("Laptops", set())
        _sellers = refurb_data.get("sellers", {}).get(country_code, {})
        _approved_phones = _sellers.get("Phones", set())
        _approved_laptops = _sellers.get("Laptops", set())

        _cc = cat_code.strip()
        _c_low = cat_path.lower()
        _is_phone_a = _cc in _phone_cats or any(w in _c_low for w in ("phone", "smartphone", "mobile"))
        _is_laptop_a = _cc in _laptop_cats or any(w in _c_low for w in ("laptop", "notebook", "macbook", "ultrabook"))

        if _is_phone_a and _approved_phones and seller_lower not in _approved_phones:
            return {
                "is_refurb": True,
                "is_compliant": False,
                "violation": {
                    "reason_type": "AI Missed Refurbished - Unapproved Refurbished Seller",
                    "reason_code": REASON_SELLER_SUPPORT,
                    "detail": f"Seller '{seller_raw}' is not authorised to list refurbished Phones in {country_code}. Contact Seller Support for approval.",
                },
            }
        if _is_laptop_a and _approved_laptops and seller_lower not in _approved_laptops:
            return {
                "is_refurb": True,
                "is_compliant": False,
                "violation": {
                    "reason_type": "AI Missed Refurbished - Unapproved Refurbished Seller",
                    "reason_code": REASON_SELLER_SUPPORT,
                    "detail": f"Seller '{seller_raw}' is not authorised to list refurbished Laptops in {country_code}. Contact Seller Support for approval.",
                },
            }

    # All 3 gates passed — fully compliant under REF-11!
    return {"is_refurb": True, "is_compliant": True, "violation": None}

