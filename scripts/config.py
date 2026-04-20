"""
Configuration and constants for the V4 Ukraine-war media analysis.
"""

from pathlib import Path

# ── Paths ──
DATA_DIR = Path(__file__).resolve().parent.parent / "data"
OUTPUT_DIR = Path(__file__).resolve().parent.parent / "output"
PLOTS_DIR = OUTPUT_DIR / "plots"

# ── Date range ──
DATE_START = "2022-02-24"
DATE_END = "2024-02-23"

# ── Columns to load (memory-efficient) ──
USECOLS = [
    "document_id", "date", "portal", "illiberal",
    "document_cap_major_label", "document_sentiment3", "document_nerw",
]

# ── Ukraine-war keyword dictionary (multilingual) ──
UKRAINE_KEYWORDS = [
    # Czech / Slovak
    "Rusko", "Putin", "Moskva", "Ukrajina", "Zelenskyj", "Kyjev",
    # Hungarian
    "Oroszország", "Putyin", "Moszkva", "Ukrajna", "Zelenszkij", "Kijev",
    # Polish (Putin already included above)
    "Rosja", "Moskwa", "Ukraina", "Zełenski", "Kijów",
]

# ── Portal metadata ──
PORTAL_COUNTRY = {
    "MF Dnes": "CZ",
    "Novinky": "CZ",
    "Magyar Nemzet": "HU",
    "Index": "HU",
    "Telex": "HU",
    "wPolityce": "PL",
    "Gazeta Wyborcza": "PL",
    "Pravda": "SK",
    "Aktuality": "SK",
}

PORTAL_ORDER = [
    "MF Dnes", "Novinky",
    "Magyar Nemzet", "Telex",
    "wPolityce", "Gazeta Wyborcza",
    "Pravda", "Aktuality",
]

# ── CAP categories for indices ──
EFI_CATEGORIES = ["macroeconomics", "energy"]
HFI_CATEGORIES = ["civil rights", "immigration", "social welfare"]

# ── Plot styling ──
COLOR_ILLIBERAL = "#e74c3c"
COLOR_LIBERAL = "#3498db"
COLOR_ILLIBERAL_LIGHT = "#f5b7b1"
COLOR_LIBERAL_LIGHT = "#aed6f1"

PORTAL_COLORS = {
    "MF Dnes": COLOR_ILLIBERAL, "Novinky": COLOR_LIBERAL,
    "Magyar Nemzet": COLOR_ILLIBERAL, "Telex": COLOR_LIBERAL,
    "wPolityce": COLOR_ILLIBERAL, "Gazeta Wyborcza": COLOR_LIBERAL,
    "Pravda": COLOR_ILLIBERAL, "Aktuality": COLOR_LIBERAL,
}

COUNTRIES = ["CZ", "HU", "PL", "SK"]
