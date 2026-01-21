# salary_parser.py
"""
Salary parsing and filtering using a deterministic regex-based pipeline.
STRICT VERSION: Hard limit of $8/hr (~$1280/mo). No buffers.

Pipeline:
1) Normalize text to lowercase.
2) Detect currency (USD/PHP) & Unit (Hour/Month).
3) Extract numbers reliably.
4) Logic/Inference:
   - Convert PHP to USD.
   - Convert Hourly to Monthly (x160).
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
    amount: float
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
    
    # 2. Convert Time Unit to Monthly
    if unit == "hour":
        val_usd = val_usd * HOURS_PER_MONTH
    elif unit == "week":
        val_usd = val_usd * 4.33
    elif unit == "year":
        val_usd = val_usd / 12.0
    
    # 3. Heuristic: If result < 100, assume hourly
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
    currency = detect_currency(text)
    unit = detect_unit(text)
    nums = extract_numbers(text)
    
    if not nums:
        return None
        
    # Take the MAX value (benefit of the doubt for ranges)
    # e.g., "$3 - $10" -> checks $10.
    amount = max(nums)

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
        is_hourly_heuristic=(monthly_usd != amount and unit == "unknown"),
        estimated_monthly_usd=monthly_usd,
        raw=raw
    )

# ---------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------

def is_salary_too_low(
    salary_str: str,
    min_monthly_usd: float = 1280.0,  # Exactly $8/hr * 160
    min_monthly_php: float = 70000.0,
    unknown_policy: str = "keep",
) -> bool:
    """
    Returns True if the salary is strictly below the threshold.
    """
    s_lower = str(salary_str).lower()
    if "negotiable" in s_lower or "doe" in s_lower or "tbd" in s_lower:
        return False

    facts = build_salary_facts(salary_str)
    
    if not facts:
        return unknown_policy == "skip"

    # STRICT CHECK: No buffer.
    # $6/hr -> $960 -> Rejected
    # $7/hr -> $1120 -> Rejected
    # $8/hr -> $1280 -> Passed
    if facts.estimated_monthly_usd < min_monthly_usd:
        logging.info(f"   📉 Salary Rejected: '{salary_str}' (~${int(facts.estimated_monthly_usd)}/mo < ${int(min_monthly_usd)})")
        return True

    return False