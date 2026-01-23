# salary_parser.py
"""
Salary parsing and filtering (STRICT $8/HR VERSION).

Thresholds:
- Hourly: $8.00
- Weekly: $320.00 (8 * 40)
- Monthly: $1,280.00 (8 * 160)

Logic Pipeline:
1. Normalize text & Extract numbers.
2. Detect Currency (USD/PHP) & Unit (Hour/Week/Month).
3. Convert EVERYTHING to a normalized Monthly USD value.
4. Compare against STRICT threshold ($1280).
"""

from __future__ import annotations

import re
import logging
from dataclasses import dataclass
from typing import Optional, List, Dict

# ---------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------

HOURS_PER_MONTH = 160.0      # Standard full-time hours (4 weeks * 40 hours)
WEEKS_PER_MONTH = 4.0        # Simplified for filtering safety
PHP_TO_USD_RATE = 56.0       # Conservative exchange rate

# STRICT DEFAULT THRESHOLD ($8/hr * 160hr = $1280/mo)
DEFAULT_MIN_USD = 1280.0 

# ---------------------------------------------------------------------
# Regex patterns
# ---------------------------------------------------------------------

CURRENCY_PATTERNS: Dict[str, re.Pattern] = {
    "php": re.compile(r"(?:\bphp\b|₱|\bpeso\b|\bpesos\b)"),
    "usd": re.compile(r"(?:\$|usd|dollar|bucks)"),
}

UNIT_PATTERNS: Dict[str, re.Pattern] = {
    "hour": re.compile(r"(?:/hr|/hour|per hour|hourly|/h\b|\bh\b)"),
    "week": re.compile(r"(?:/wk|/week|per week|weekly|/w\b)"),
    "month": re.compile(r"(?:/mo|/month|per month|monthly|/m\b)"),
    "year": re.compile(r"(?:/yr|/year|per year|yearly|/y\b|annum)"),
}

# Catch format like "2$ hour" which is common garbage
BAD_FORMAT_HOURLY = re.compile(r"\d+\s*\$\s*hour")

# ---------------------------------------------------------------------
# Data Structures
# ---------------------------------------------------------------------

@dataclass
class SalaryFacts:
    currency: str
    unit: str
    amount: float
    estimated_monthly_usd: float
    raw: str

# ---------------------------------------------------------------------
# Core Logic
# ---------------------------------------------------------------------

def detect_currency(text: str) -> str:
    if CURRENCY_PATTERNS["php"].search(text):
        return "php"
    if CURRENCY_PATTERNS["usd"].search(text):
        return "usd"
    return "unknown"

def detect_unit(text: str) -> str:
    if BAD_FORMAT_HOURLY.search(text):
        return "hour"
    for unit, pattern in UNIT_PATTERNS.items():
        if pattern.search(text):
            return unit
    return "unknown"

def extract_numbers(text: str) -> List[float]:
    clean_text = text.replace(",", "")
    matches = re.findall(r"(\d+(?:\.\d+)?)", clean_text)
    nums = []
    for m in matches:
        try:
            val = float(m)
            nums.append(val)
        except ValueError:
            pass
    return nums

def calculate_monthly_usd(amount: float, currency: str, unit: str) -> float:
    val_usd = amount

    # 1. Convert Currency to USD
    if currency == "php":
        val_usd = amount / PHP_TO_USD_RATE
    
    # ---------------------------------------------------------
    # SMART UNIT CORRECTION (Prevent Logic Errors)
    # ---------------------------------------------------------
    
    # Case A: Logic thinks it's Hourly, but number is huge.
    # e.g., "330" USD (detected as hour by mistake or no unit).
    # If someone asks for > $100/hr ($16,000/mo) for a VA role, it's likely a monthly salary typo.
    if unit == "hour" and val_usd > 100: 
        logging.debug(f"   ⚠️ Heuristic: ${val_usd}/hr is improbably high. Treating as MONTHLY.")
        unit = "month"

    # Case B: Logic thinks it's Monthly, but number is tiny.
    # e.g., "$8" (detected as month). Nobody works for $8/month. It's likely hourly.
    elif unit == "month" and val_usd < 100:
        logging.debug(f"   ⚠️ Heuristic: ${val_usd}/mo is improbably low. Treating as HOURLY.")
        unit = "hour"
        
    # Case C: Unknown unit
    elif unit == "unknown":
        if val_usd < 100: # Below $100 -> Assume Hourly
            unit = "hour"
        else:             # Above $100 -> Assume Monthly
            unit = "month"

    # ---------------------------------------------------------
    # 2. Normalize to Monthly USD
    # ---------------------------------------------------------
    if unit == "hour":
        val_usd = val_usd * HOURS_PER_MONTH  # x 160
    elif unit == "week":
        val_usd = val_usd * WEEKS_PER_MONTH  # x 4
    elif unit == "year":
        val_usd = val_usd / 12.0
    
    return val_usd

# ---------------------------------------------------------------------
# Main Pipeline
# ---------------------------------------------------------------------

def build_salary_facts(raw: str) -> Optional[SalaryFacts]:
    if not raw:
        return None
        
    text = raw.lower().strip()
    currency = detect_currency(text)
    unit = detect_unit(text)
    nums = extract_numbers(text)
    
    if not nums:
        return None
        
    # Take the MAX value (benefit of the doubt for ranges like "$500 - $1000")
    amount = max(nums)

    # Fallback currency logic
    if currency == "unknown":
        if amount > 5000: 
            currency = "php"
        else:
            currency = "usd"

    monthly_usd = calculate_monthly_usd(amount, currency, unit)

    return SalaryFacts(
        currency=currency,
        unit=unit,
        amount=amount,
        estimated_monthly_usd=monthly_usd,
        raw=raw
    )

# ---------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------

def is_salary_too_low(
    salary_str: str,
    min_monthly_usd: float = DEFAULT_MIN_USD, # Default is now $1280
    min_monthly_php: float = 70000.0,
    unknown_policy: str = "keep",
) -> bool:
    """
    Returns True if the salary is strictly below $8/hr ($1280/mo).
    """
    s_lower = str(salary_str).lower()
    
    # 1. Skip logic for text-only salaries (Negotiable/DOE)
    if not salary_str or "negotiable" in s_lower or "doe" in s_lower or "tbd" in s_lower:
        return False

    facts = build_salary_facts(salary_str)
    
    if not facts:
        # If parsable numbers are missing, decide based on policy
        return unknown_policy == "skip"

    # 2. THE CHECK
    # Strict comparison against threshold
    if facts.estimated_monthly_usd < min_monthly_usd:
        logging.info(f"   📉 Salary Rejected: '{salary_str}' (~${int(facts.estimated_monthly_usd)}/mo < ${int(min_monthly_usd)})")
        return True

    return False