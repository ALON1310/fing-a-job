# tests/test_salary_parser.py
import pytest
import salary_parser


# -----------------------------
# 1) Currency detection tests
# -----------------------------

@pytest.mark.parametrize(
    "text, expected",
    [
        ("$5 per hour", "usd"),
        ("USD 1000 / month", "usd"),
        ("Up to 3,000USD/month", "usd"),  # FIXED by new USD regex
        ("US$250/week", "usd"),
        ("1,000 dollars", "usd"),
        ("Php 15,000", "php"),
        ("PHP 25000 / month", "php"),
        ("₱25,000 - 30,000", "php"),
        ("15,000 pesos monthly", "php"),
        ("salary negotiable", "unknown"),
        ("25,000 - 30,000", "unknown"),
        ("", "unknown"),
    ],
)
def test_detect_currency(text, expected):
    assert salary_parser.detect_currency(text) == expected


# -----------------------------
# 2) Unit detection tests
# -----------------------------

@pytest.mark.parametrize(
    "text, expected",
    [
        ("$5 per hour", "hour"),
        ("$5/hour", "hour"),
        ("$1.50 USD/HR", "hour"),
        ("$250/week", "week"),
        ("$250 per week", "week"),
        ("600-800$/monthly", "month"),
        ("over $600/mo", "month"),
        ("Up to 3,000USD/month", "month"),
        ("$15,000/year start pay, plus bonuses", "year"),
        ("25,000 - 30,000", "unknown"),
        ("", "unknown"),
    ],
)
def test_detect_unit(text, expected):
    assert salary_parser.detect_unit(text) == expected


# -----------------------------
# 3) Number extraction tests
# -----------------------------

@pytest.mark.parametrize(
    "text, expected",
    [
        ("$1.50 USD/HR", [1.50]),  # FIXED by allowing '.' in number extraction
        ("$250/week", [250.0]),
        ("$900-$1,000/month", [900.0, 1000.0]),
        ("600-800$/monthly", [600.0, 800.0]),
        ("Up to 3,000USD/month", [3000.0]),
        ("over $600/mo", [600.0]),
        ("Php 15,000", [15000.0]),
        ("25,000 - 30,000", [25000.0, 30000.0]),
        ("negotiable", []),
        ("", []),
    ],
)
def test_extract_numbers(text, expected):
    assert salary_parser.extract_numbers(text) == expected


# -----------------------------
# 4) build_salary_facts tests
# -----------------------------

@pytest.mark.parametrize(
    "raw, exp_currency, exp_unit, exp_low_raw, exp_high_raw",
    [
        ("$1.50 USD/HR", "usd", "hour", 1.50, 1.50),
        ("$250/week", "usd", "week", 250.0, 250.0),
        ("$900-$1,000/month", "usd", "month", 900.0, 1000.0),
        ("600-800$/monthly", "usd", "month", 600.0, 800.0),
        ("over $600/mo", "usd", "month", 600.0, None),
        ("Up to 3,000USD/month", "usd", "month", None, 3000.0),

        # NOTE: these keep unit unknown because your pipeline is regex-only for unit
        ("Php 15,000", "php", "unknown", 15000.0, 15000.0),
        ("₱25,000 - 30,000", "php", "unknown", 25000.0, 30000.0),

        ("negotiable", "unknown", "unknown", None, None),
    ],
)
def test_build_salary_facts(raw, exp_currency, exp_unit, exp_low_raw, exp_high_raw):
    facts = salary_parser.build_salary_facts(raw)
    assert facts.currency == exp_currency
    assert facts.unit == exp_unit
    assert facts.low_raw == exp_low_raw
    assert facts.high_raw == exp_high_raw


# -----------------------------
# 5) is_salary_too_low tests
# -----------------------------

@pytest.mark.parametrize(
    "raw, min_usd, min_php, policy, expected",
    [
        # ---- original set ----
        ("$1000/month", 900, 50000, "keep", False),
        ("$400/mo", 900, 50000, "keep", True),

        ("$5/hour", 900, 50000, "keep", True),
        ("$6/hour", 900, 50000, "keep", False),

        ("$250/week", 900, 50000, "keep", False),

        ("$15,000/year", 900, 50000, "keep", False),

        ("$900-$1,000/month", 900, 50000, "keep", False),
        ("$800-$1,000/month", 900, 50000, "keep", True),

        ("over $600/mo", 900, 50000, "keep", True),
        ("Up to 3,000USD/month", 900, 50000, "keep", False),
        ("Up to $500/mo", 900, 50000, "keep", True),

        # PHP unknown unit -> still policy-based (no monthly conversion)
        ("Php 15,000", 900, 50000, "keep", False),
        ("Php 15,000", 900, 50000, "skip", True),

        # ---- UPDATED because of your NEW inference rules ----
        # Unknown currency + 25,000 => inferred as PHP/month -> 25k < 50k => too low
        ("25,000 - 30,000", 900, 50000, "keep", True),
        ("25,000 - 30,000", 900, 50000, "skip", True),

        # "negotiable" / empty => no numbers => remains unknown => throw away => True
        ("negotiable", 900, 50000, "keep", True),
        ("negotiable", 900, 50000, "skip", True),
        ("", 900, 50000, "keep", True),
        ("", 900, 50000, "skip", True),

        # ---- +10 extra edge cases ----

        # 1) USD monthly with spaces and comma
        (" USD   1,200   /   month ", 900, 50000, "keep", False),

        # 2) US$ + "per week"
        ("US$250 per week", 900, 50000, "keep", False),

        # 3) hourly variant: "per hr" + decimals
        ("$4.99 per hr", 900, 50000, "keep", True),

        # 4) weekly variant: "weekly"
        ("$300 weekly", 900, 50000, "keep", False),

        # 5) yearly variant: "annually" (exactly 900/mo)
        ("$10,800 annually", 900, 50000, "keep", False),

        # 6) range with "to" => your number normalization turns it into 2 nums (850, 950),
        # but no unit conversion if "/ month" contains space - your new regex supports it.
        # Monthly, low=850 < 900 => too low
        ("$850 to $950 / month", 900, 50000, "keep", True),

        # 7) "upto" without space
        ("upto $899/mo", 900, 50000, "keep", True),

        # 8) PHP monthly explicit unit (requested)
        ("₱60,000/mo", 900, 50000, "keep", False),

        # 9) only-number case (unknown currency) => inferred USD/hour (<100)
        # 75/hour => 75*173.2 = 12990 > 900 => NOT too low
        ("75", 900, 50000, "keep", False),
        ("75", 900, 50000, "skip", False),

        # 10) "throw away" case: unknown and out of inference ranges
        ("500000", 900, 50000, "keep", True),
    ],
)
def test_is_salary_too_low(raw, min_usd, min_php, policy, expected):
    result = salary_parser.is_salary_too_low(
        raw,
        min_usd,
        min_php,
        unknown_policy=policy,
    )
    assert result is expected
