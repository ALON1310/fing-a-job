# salary_parser.py
"""
Salary parsing and filtering using a deterministic regex-based pipeline.

Pipeline:
1) Normalize text to lowercase.
2) Detect currency (USD/PHP) using regex.
3) Detect time unit (hour/week/month/year) using regex.
4) Extract numbers.
5) Logic/Inference for unknown currencies.

Updates:
- All regexes are now lowercase only (we convert input to .lower() first).
- Added support for 'h' (hour) and 'm' (month).
- Supports decimal extraction.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Optional, List, Dict, Tuple

# ---------------------------------------------------------------------
# Conversion constants
# ---------------------------------------------------------------------

HOURS_PER_MONTH = 40 * 4.33  # ~173.2 hours/month
WEEKS_PER_MONTH = 4.33       # ~4.33 weeks/month


# ---------------------------------------------------------------------
# Regex patterns: currency detection (USD/PHP)
# All patterns are LOWERCASE. Input is converted to lower() before matching.
# ---------------------------------------------------------------------

# Lookbehind/Lookahead for 'usd' to ensure it's not part of another word (like "used")
_USD_TOKEN = r"(?<![a-z])usd(?![a-z])"

CURRENCY_PATTERNS: Dict[str, re.Pattern] = {
    "php": re.compile(
        r"(?:\bphp\b|₱|\bpeso\b|\bpesos\b|\bphilippine\s+peso\b)",
    ),
    "usd": re.compile(
        # Matches: $, usd, u.s.d, us$, dollar(s)
        # Also matches "us" if attached to digits (50us) or word boundary (us)
        rf"(?:\$\s*|{_USD_TOKEN}|\bu\.s\.d\b|\bus\$\b|\bdollar\b|\bdollars\b|\b\d*us\b)",
    ),
}


def detect_currency(text: str) -> str:
    """
    Detect currency using regex only.
    Converts text to lowercase first.
    """
    if not text:
        return "unknown"
    
    # FORCE LOWERCASE
    t = text.lower()

    if CURRENCY_PATTERNS["php"].search(t):
        return "php"
    if CURRENCY_PATTERNS["usd"].search(t):
        return "usd"
    return "unknown"


# ---------------------------------------------------------------------
# Regex patterns: unit detection (hour/week/month/year)
# Updated to support 'h' and 'm'
# ---------------------------------------------------------------------

UNIT_PATTERNS: Dict[str, re.Pattern] = {
    "hour": re.compile(
        # Matches: per hour, /hour, hourly, /hr, hr, /h, per h, or explicit 'h' at boundary
        r"(?:per\s*hour|/\s*hour|\bhourly\b|/\s*hr\b|\bhr\b|/\s*h\b|\bh\b)", 
    ),
    "week": re.compile(
        r"(?:per\s*week|/\s*week|\bweekly\b|/\s*wk\b|\bwk\b|\bweek\b|/\s*w\b|\bw\b)", 
    ),
    "month": re.compile(
        # Matches: per month, /month, monthly, /mo, mo, /m, per m, pr mo
        r"(?:per\s*month|/\s*month|\bmonthly\b|/\s*mo\b|\bmo\b|\bmonth\b|pr\s*mo|/\s*m\b|\bm\b)", 
    ),
    "year": re.compile(
        r"(?:per\s*year|/\s*year|\byearly\b|\bannual\b|\bannually\b|\byear\b|/\s*y\b|\by\b)", 
    ),
}

OVER_PATTERN = re.compile(r"(?:\bover\b|\bat\s*least\b|\bfrom\b|\bminimum\b|\bmin\b)")
UPTO_PATTERN = re.compile(r"(?:\bup\s*to\b|\bupto\b|\bmaximum\b|\bmax\b|\bno\s+more\s+than\b)")


def detect_unit(text: str) -> str:
    """
    Detect time unit using regex only.
    Converts text to lowercase first.
    """
    if not text:
        return "unknown"
    
    # FORCE LOWERCASE
    t = text.lower()

    for unit, pattern in UNIT_PATTERNS.items():
        if pattern.search(t):
            return unit
    return "unknown"


# ---------------------------------------------------------------------
# Number extraction
# ---------------------------------------------------------------------

_NON_DIGIT_COMMA_DOT = re.compile(r"[^\d,\.]+")
_MULTI_SPACES = re.compile(r"\s+")


def normalize_for_numbers(text: str) -> str:
    if not text:
        return ""
    
    # We don't strictly need lower() for digits, but consistent is good
    t = text.lower() 

    cleaned = _NON_DIGIT_COMMA_DOT.sub(" ", t)
    cleaned = _MULTI_SPACES.sub(" ", cleaned).strip()
    return cleaned


def _is_valid_number_token(token: str) -> bool:
    if not token:
        return False
    # Must contain at least one digit
    if not any(ch.isdigit() for ch in token):
        return False
    # At most one dot
    if token.count(".") > 1:
        return False
    # Token should not be only punctuation
    stripped = token.replace(",", "").replace(".", "")
    return stripped.isdigit()


def extract_numbers(text: str, max_numbers: int = 2) -> List[float]:
    cleaned = normalize_for_numbers(text)
    if not cleaned:
        return []

    parts = cleaned.split(" ")
    nums: List[float] = []

    for part in parts:
        if not _is_valid_number_token(part):
            continue

        part_no_commas = part.replace(",", "")
        try:
            nums.append(float(part_no_commas))
        except ValueError:
            continue

        if len(nums) >= max_numbers:
            break

    return nums


# ---------------------------------------------------------------------
# Salary facts & Inference
# ---------------------------------------------------------------------

@dataclass
class SalaryFacts:
    currency: str
    unit: str
    low_raw: Optional[float]
    high_raw: Optional[float]
    low_monthly: Optional[float]
    high_monthly: Optional[float]
    has_over: bool
    has_upto: bool
    raw: str


def _convert_to_monthly(value: Optional[float], unit: str) -> Optional[float]:
    if value is None:
        return None
    if unit == "hour":
        return value * HOURS_PER_MONTH
    if unit == "week":
        return value * WEEKS_PER_MONTH
    if unit == "year":
        return value / 12.0
    if unit == "month":
        return value
    return None


def _representative_value(low_raw: Optional[float], high_raw: Optional[float]) -> Optional[float]:
    return low_raw if low_raw is not None else high_raw


def _infer_unknown_currency_and_unit(
    currency: str,
    unit: str,
    low_raw: Optional[float],
    high_raw: Optional[float],
) -> Tuple[str, str]:
    if currency != "unknown":
        return currency, unit

    v = _representative_value(low_raw, high_raw)
    if v is None:
        return currency, unit

    # PH monthly typical range
    if 15000 <= v <= 100000:
        return "php", ("month" if unit == "unknown" else unit)

    # USD inference
    if v < 100:
        return "usd", ("hour" if unit == "unknown" else unit)
    if 100 <= v < 400:
        return "usd", ("week" if unit == "unknown" else unit)
    if 400 <= v <= 4000:
        return "usd", ("month" if unit == "unknown" else unit)

    return currency, unit


def build_salary_facts(raw: str) -> SalaryFacts:
    # FORCE LOWERCASE for patterns
    text = (raw or "").lower()
    
    currency = detect_currency(text)
    unit = detect_unit(text)

    has_over = bool(OVER_PATTERN.search(text))
    has_upto = bool(UPTO_PATTERN.search(text))

    nums = extract_numbers(text, max_numbers=2)

    low_raw: Optional[float] = None
    high_raw: Optional[float] = None

    if len(nums) == 0:
        low_raw, high_raw = None, None
    elif len(nums) == 1:
        v = nums[0]
        if has_over:
            low_raw, high_raw = v, None
        elif has_upto:
            low_raw, high_raw = None, v
        else:
            low_raw, high_raw = v, v
    else:
        low_raw, high_raw = min(nums[0], nums[1]), max(nums[0], nums[1])

    currency, unit = _infer_unknown_currency_and_unit(currency, unit, low_raw, high_raw)

    low_monthly = _convert_to_monthly(low_raw, unit)
    high_monthly = _convert_to_monthly(high_raw, unit)

    return SalaryFacts(
        currency=currency,
        unit=unit,
        low_raw=low_raw,
        high_raw=high_raw,
        low_monthly=low_monthly,
        high_monthly=high_monthly,
        has_over=has_over,
        has_upto=has_upto,
        raw=raw,
    )


# ---------------------------------------------------------------------
# Filtering logic
# ---------------------------------------------------------------------

def is_salary_too_low(
    raw: str,
    min_monthly_usd: float,
    min_monthly_php: float,
    unknown_policy: str = "keep",
) -> bool:
    facts = build_salary_facts(raw)

    if facts.currency not in ("usd", "php"):
        return True # Throw away unknown currency

    if facts.low_monthly is None and facts.high_monthly is None:
        return unknown_policy == "skip"

    min_required = min_monthly_usd if facts.currency == "usd" else min_monthly_php

    if facts.low_monthly is not None and facts.low_monthly < min_required:
        return True

    if facts.low_monthly is None and facts.high_monthly is not None and facts.high_monthly < min_required:
        return True

    return False