# salary_parser.py
"""
Salary parsing and filtering using a deterministic regex-based pipeline.
IMPROVED VERSION: Aggressive normalization to Monthly USD.
STRICTNESS UPDATE: Minimum base is set to $8/hr (~$1280/mo).

Pipeline:
1) Normalize text to lowercase.
2) Detect currency (USD/PHP) & Unit (Hour/Month).
3) Extract numbers reliably (handling '2$').
4) Logic/Inference:
   - Convert PHP to USD.
   - Convert Hourly to Monthly (x160).
   - Heuristic: If value is too low (<$100), assume it's hourly.
5) Final Threshold Check.
"""

from __future__ import annotations

import re
import logging
from dataclasses import dataclass
from typing import Optional, List, Dict

# ---------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------

HOURS_PER_MONTH = 160.0      # Standard full-time hours
PHP_TO_USD_RATE = 56.0       # Average exchange rate (1 USD = ~56 PHP)

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

# Special catch for "2$ hour" format
BAD_FORMAT_HOURLY = re.compile(r"\d+\s*\$\s*hour")

# ---------------------------------------------------------------------
# Data Structures
# ---------------------------------------------------------------------

@dataclass
class SalaryFacts:
    currency: str
    unit: str
    amount: float  # The representative amount (max of range)
    is_hourly_heuristic: bool
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
    # Explicit check for "2$ hour" type errors
    if BAD_FORMAT_HOURLY.search(text):
        return "hour"
        
    for unit, pattern in UNIT_PATTERNS.items():
        if pattern.search(text):
            return unit
    return "unknown"

def extract_numbers(text: str) -> List[float]:
    """
    Robust number extraction. 
    Handles '20,000', '1.5', and '$2' correctly.
    """
    # Remove commas to simplify parsing
    clean_text = text.replace(",", "")
    # Find all numbers (integer or float)
    matches = re.findall(r"(\d+(?:\.\d+)?)", clean_text)
    
    nums = []
    for m in matches:
        try:
            val = float(m)
            # Filter out year-like numbers (e.g. 2024) unless context implies otherwise
            # But for salary, sometimes 2000 is valid. Let's keep it simple.
            nums.append(val)
        except ValueError:
            pass
    return nums

def calculate_monthly_usd(amount: float, currency: str, unit: str) -> float:
    """
    Converts any amount to Monthly USD.
    """
    val_usd = amount

    # 1. Convert Currency to USD
    if currency == "php":
        val_usd = amount / PHP_TO_USD_RATE
    
    # 2. Convert Time Unit to Monthly
    if unit == "hour":
        val_usd = val_usd * HOURS_PER_MONTH
    elif unit == "week":
        val_usd = val_usd * 4.33
    elif unit == "year":
        val_usd = val_usd / 12.0
    
    # 3. Heuristic: If the result is absurdly low (e.g., $5), it was likely hourly
    # even if the unit wasn't detected explicitly.
    if val_usd < 100.0 and val_usd > 0:
        val_usd = val_usd * HOURS_PER_MONTH
        
    return val_usd

# ---------------------------------------------------------------------
# Main Pipeline
# ---------------------------------------------------------------------

def build_salary_facts(raw: str) -> Optional[SalaryFacts]:
    if not raw:
        return None
        
    text = raw.lower().strip()
    
    # 1. Detect Metadata
    currency = detect_currency(text)
    unit = detect_unit(text)
    
    # 2. Extract Numbers
    nums = extract_numbers(text)
    if not nums:
        return None
        
    # Take the MAX value to be permissive (benefit of the doubt)
    # e.g., "500-1000" -> we check 1000 against the threshold.
    amount = max(nums)

    # 3. Default currency to USD if unknown but looks like a number
    if currency == "unknown":
        # Heuristic: Large numbers usually PHP, small usually USD
        if amount > 5000: 
            currency = "php"
        else:
            currency = "usd"

    # 4. Calculate Final Metric
    monthly_usd = calculate_monthly_usd(amount, currency, unit)

    return SalaryFacts(
        currency=currency,
        unit=unit,
        amount=amount,
        is_hourly_heuristic=(monthly_usd != amount and unit == "unknown"),
        estimated_monthly_usd=monthly_usd,
        raw=raw
    )

# ---------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------

def is_salary_too_low(
    salary_str: str,
    min_monthly_usd: float = 1280.0,  # <--- UPDATED: $8/hr * 160h = $1280
    min_monthly_php: float = 70000.0, # Updated to match approx $1280 USD
    unknown_policy: str = "keep",
) -> bool:
    """
    Returns True if the salary is DEFINITELY too low.
    Default threshold is now ~$1280/month (which equals $8/hour).
    """
    # Special bypass for text-only salaries
    s_lower = str(salary_str).lower()
    if "negotiable" in s_lower or "doe" in s_lower or "tbd" in s_lower:
        return False

    facts = build_salary_facts(salary_str)
    
    if not facts:
        # If we couldn't parse any number, follow policy
        return unknown_policy == "skip"

    # BUFFER: We allow 5% margin of error (so ~$7.60/hr might slip through, but $7.00 won't)
    threshold = min_monthly_usd * 0.95
    
    if facts.estimated_monthly_usd < threshold:
        logging.info(f"   📉 Salary Rejected: '{salary_str}' (~${int(facts.estimated_monthly_usd)}/mo < ${int(min_monthly_usd)})")
        return True

    return False