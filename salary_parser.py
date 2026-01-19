# salary_parser.py
"""
Salary parsing and filtering using a deterministic regex-based pipeline.

Pipeline:
1) Detect currency (USD/PHP) using regex.
2) Detect time unit (hour/week/month/year) using regex.
3) Extract 1-2 numbers using regex normalization:
   - Replace anything that is NOT digit / comma / dot with a space.
   - Collapse spaces.
   - Split and parse numbers (commas removed).
4) Store extracted facts and decide if salary is too low using currency-specific thresholds.

NEW (your requested behavior):
- If currency is unknown but we have numbers:
  1) <100           -> treat as USD/hour
  2) 100..400       -> treat as USD/week
  3) 400..4000      -> treat as USD/month
  4) 15000..100000  -> treat as PHP/month  (common PH monthly range)
  else              -> "throw away" (filter out)

Upgrades requested now:
- Currency detection: detect "3,000USD" (USD attached to digits) as USD.
- Number extraction: support decimals like "$1.50 USD/HR" -> [1.5].
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
# ---------------------------------------------------------------------

# NOTE:
# We want to detect "USD" even when it's attached to digits like "3000USD".
# Using (?<![A-Za-z])USD(?![A-Za-z]) allows digits/commas before USD, but blocks letters.
_USD_TOKEN = r"(?<![A-Za-z])usd(?![A-Za-z])"

CURRENCY_PATTERNS: Dict[str, re.Pattern] = {
    "php": re.compile(
        r"(?:\bphp\b|₱|\bpeso\b|\bpesos\b|\bphilippine\s+peso\b)",
        re.IGNORECASE,
    ),
    "usd": re.compile(
        rf"(?:\$\s*|{_USD_TOKEN}|\bu\.s\.d\b|\bus\$\b|\bdollar\b|\bdollars\b)",
        re.IGNORECASE,
    ),
}


def detect_currency(text: str) -> str:
    """
    Detect currency using regex only.

    Args:
        text: Raw salary text.

    Returns:
        'php', 'usd', or 'unknown'
    """
    if not text:
        return "unknown"

    # Prefer explicit PHP markers first (avoid '$' catching PHP strings that also include '$')
    if CURRENCY_PATTERNS["php"].search(text):
        return "php"
    if CURRENCY_PATTERNS["usd"].search(text):
        return "usd"
    return "unknown"


# ---------------------------------------------------------------------
# Regex patterns: unit detection (hour/week/month/year)
# FIX: allow spaces after '/', include plain "week/month/year" tokens too
# ---------------------------------------------------------------------

UNIT_PATTERNS: Dict[str, re.Pattern] = {
    "hour": re.compile(r"(?:per\s*hour|/\s*hour|\bhourly\b|/\s*hr\b|\bhr\b)", re.IGNORECASE),
    "week": re.compile(r"(?:per\s*week|/\s*week|\bweekly\b|/\s*wk\b|\bwk\b|\bweek\b)", re.IGNORECASE),
    "month": re.compile(r"(?:per\s*month|/\s*month|\bmonthly\b|/\s*mo\b|\bmo\b|\bmonth\b)", re.IGNORECASE),
    "year": re.compile(r"(?:per\s*year|/\s*year|\byearly\b|\bannual\b|\bannually\b|\byear\b)", re.IGNORECASE),
}

OVER_PATTERN = re.compile(r"(?:\bover\b|\bat\s*least\b|\bfrom\b|\bminimum\b|\bmin\b)", re.IGNORECASE)
UPTO_PATTERN = re.compile(r"(?:\bup\s*to\b|\bupto\b|\bmaximum\b|\bmax\b|\bno\s+more\s+than\b)", re.IGNORECASE)


def detect_unit(text: str) -> str:
    """
    Detect time unit using regex only.

    Args:
        text: Raw salary text.

    Returns:
        'hour', 'week', 'month', 'year', or 'unknown'
    """
    if not text:
        return "unknown"

    for unit, pattern in UNIT_PATTERNS.items():
        if pattern.search(text):
            return unit
    return "unknown"


# ---------------------------------------------------------------------
# Number extraction (now supports decimals)
# ---------------------------------------------------------------------

# Keep digits, commas and dot (decimal point). Everything else becomes a space.
_NON_DIGIT_COMMA_DOT = re.compile(r"[^\d,\.]+")
_MULTI_SPACES = re.compile(r"\s+")


def normalize_for_numbers(text: str) -> str:
    """
    Keep digits, commas, and '.' only; everything else becomes a space.
    Then collapse multiple spaces.

    Examples:
        "$900-$1,000/month" -> "900 1,000"
        "Php 15,000" -> "15,000"
        "$1.50 USD/HR" -> "1.50"
        "Up to 3,000USD/month" -> "3,000"

    Args:
        text: Raw text

    Returns:
        A cleaned string containing numeric chunks separated by single spaces.
    """
    if not text:
        return ""

    cleaned = _NON_DIGIT_COMMA_DOT.sub(" ", text)
    cleaned = _MULTI_SPACES.sub(" ", cleaned).strip()
    return cleaned


def _is_valid_number_token(token: str) -> bool:
    """
    Validate a numeric token after normalization.

    We allow:
      - digits
      - commas (thousands separators)
      - at most one dot

    Reject tokens like:
      - "." or ","
      - "1..2"
      - "1,2,3.4.5"
    """
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
    """
    Extract up to `max_numbers` numeric values from text.

    Steps:
    1) Replace any non-digit/non-comma/non-dot chars with spaces
    2) Collapse spaces
    3) Split
    4) Parse floats after removing commas

    Args:
        text: Raw salary text
        max_numbers: Max amount of numbers to return (default 2)

    Returns:
        List of floats (length 0..max_numbers)
    """
    cleaned = normalize_for_numbers(text)
    if not cleaned:
        return []

    parts = cleaned.split(" ")
    nums: List[float] = []

    for part in parts:
        if not _is_valid_number_token(part):
            continue

        # Remove commas: "1,000.50" -> "1000.50"
        part_no_commas = part.replace(",", "")
        try:
            nums.append(float(part_no_commas))
        except ValueError:
            continue

        if len(nums) >= max_numbers:
            break

    return nums


# ---------------------------------------------------------------------
# Salary facts
# ---------------------------------------------------------------------

@dataclass
class SalaryFacts:
    """
    Facts extracted from a salary string.

    - currency: 'usd', 'php', or 'unknown'
    - unit: 'hour', 'week', 'month', 'year', or 'unknown'
    - low_raw/high_raw: numeric bounds in the detected unit (not monthly yet)
    - low_monthly/high_monthly: numeric bounds converted to monthly if possible
    - has_over/has_upto: whether text suggests "over" or "up to"
    - raw: original salary text (for debugging/logging)
    """
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
    """
    Convert a numeric value to a monthly value given a unit.
    """
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
    """
    Pick a single value for inference heuristics:
    - prefer low if exists; else use high
    """
    return low_raw if low_raw is not None else high_raw


def _infer_unknown_currency_and_unit(
    currency: str,
    unit: str,
    low_raw: Optional[float],
    high_raw: Optional[float],
) -> Tuple[str, str]:
    """
    Your requested behavior:
    - If currency unknown, infer by magnitude:
      <100           -> USD/hour
      100..400       -> USD/week
      400..4000      -> USD/month
      15000..100000  -> PHP/month
      else           -> remain unknown (caller will "throw away")
    """
    if currency != "unknown":
        return currency, unit

    v = _representative_value(low_raw, high_raw)
    if v is None:
        return currency, unit

    # PH monthly typical range (even if no PHP marker)
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
    """
    Build SalaryFacts using the pipeline + your inference rules for unknown currency.
    """
    text = raw or ""
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

    # Infer unknown currency/unit by your rules
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
    """
    Decide whether a job should be filtered out due to low salary.

    Key behavior:
    - If currency still unknown after inference: ALWAYS filter out (throw away).
    - Otherwise compare monthly salary against the currency-specific threshold.
    - If monthly conversion isn't possible, fall back to unknown_policy.
    """
    facts = build_salary_facts(raw)

    # If still unknown after inference -> throw away
    if facts.currency not in ("usd", "php"):
        return True

    # If we still can't compare (no monthly numbers), fall back to policy
    if facts.low_monthly is None and facts.high_monthly is None:
        return unknown_policy == "skip"

    min_required = min_monthly_usd if facts.currency == "usd" else min_monthly_php

    # Lower bound check (conservative)
    if facts.low_monthly is not None and facts.low_monthly < min_required:
        return True

    # Upper bound only ("up to")
    if facts.low_monthly is None and facts.high_monthly is not None and facts.high_monthly < min_required:
        return True

    return False
