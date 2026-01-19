import pytest
from datetime import datetime, timedelta

# Import the utility functions to be tested
from utils import (
    extract_email, 
    normalize_text, 
    colnum_to_a1, 
    ensure_columns, 
    get_days_diff,
    get_timestamp_iso
)

# ---------------------------------------------------------
# Unit Tests
# ---------------------------------------------------------

def test_extract_email():
    """Verifies the email extraction logic (Regex behavior)."""
    # Standard extraction
    assert extract_email("Contact alon@test.com now") == "alon@test.com"
    
    # Edge cases: No email, empty string, None
    assert extract_email("NO EMAIL HERE") == ""
    assert extract_email("") == ""
    assert extract_email(None) == ""
    
    # Case Insensitivity check
    assert extract_email("Mail: User@Domain.Co.Il") == "User@Domain.Co.Il"

def test_normalize_text():
    """Verifies that extra whitespace and None values are handled correctly."""
    text = "  Hello   World  "
    assert normalize_text(text) == "Hello World"
    
    # Edge cases
    assert normalize_text(None) == ""
    assert normalize_text("") == ""

def test_colnum_to_a1():
    """Verifies the conversion of numerical indices to Excel A1 notation."""
    assert colnum_to_a1(1) == "A"
    assert colnum_to_a1(26) == "Z"
    assert colnum_to_a1(27) == "AA"
    
    # Verify that invalid input (0 or negative) raises a ValueError
    with pytest.raises(ValueError):
        colnum_to_a1(0)

def test_ensure_columns():
    """Verifies that missing columns are appended without creating duplicates."""
    current_headers = ["Name", "Email"]
    required_headers = ["Email", "Status", "Date"]
    
    updated = ensure_columns(current_headers, required_headers)
    
    # Validation: Should contain originals + new ones
    assert "Name" in updated
    assert "Email" in updated
    assert "Status" in updated
    assert "Date" in updated
    assert len(updated) == 4 # Expected: Name, Email, Status, Date

def test_get_days_diff():
    """Verifies date difference calculation logic."""
    # 1. Today's date -> Difference should be 0
    today = datetime.now().strftime("%Y-%m-%d")
    assert get_days_diff(today) == 0
    
    # 2. Yesterday's date -> Difference should be 1
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    assert get_days_diff(yesterday) == 1
    
    # 3. Full ISO format (including time)
    iso_date = (datetime.now() - timedelta(days=5)).strftime("%Y-%m-%dT10:00:00")
    assert get_days_diff(iso_date) == 5
    
    # 4. Edge Cases (Empty or Invalid date should return safe fallback 999)
    assert get_days_diff("") == 999
    assert get_days_diff("Not A Date") == 999
    assert get_days_diff(None) == 999

def test_get_timestamp_iso():
    """Verifies that the generated timestamp is in valid UTC ISO format."""
    ts = get_timestamp_iso()
    
    # Ensure UTC format markers exist ('T' separator and 'Z' timezone)
    assert "T" in ts
    assert "Z" in ts
    
    # Verify it is parsable as a valid datetime object
    # No exception raised means the format is valid
    assert datetime.strptime(ts, "%Y-%m-%dT%H:%M:%SZ")