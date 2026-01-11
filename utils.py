"""
utils.py
Shared utility functions for Platonics CRM.
Contains generic logic for logging, time, text processing, and spreadsheet helpers.
"""

import logging
import re
import sys
from datetime import datetime, timezone


# --- 1. LOGGING SETUP ---
def setup_logging():
    """Sets up the global logging configuration consistent across all agents."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        stream=sys.stdout  # Explicitly print to stdout
    )

# --- 2. TIME HELPER ---
def get_timestamp_iso():
    """Returns current UTC time in ISO format (e.g., 2026-01-08T15:30:00Z)."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

# --- 3. TEXT & EMAIL HELPERS ---
def extract_email(text):
    """Extracts the first email address found in a text string using Regex."""
    if not text:
        return ""
    # The exact regex used in your current sender_agent
    m = re.search(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}", text, re.I)
    return m.group(0) if m else ""

def normalize_text(text):
    """Cleans text: removes extra spaces and strips whitespace."""
    # Useful for headers and descriptions
    return re.sub(r"\s+", " ", (text or "").strip())

# --- 4. SPREADSHEET HELPERS ---
def colnum_to_a1(n):
    """Converts a column number (1, 2, 27) to A1 notation (A, B, AA)."""
    if n < 1: 
        raise ValueError("Column number must be >= 1")
    letters = ""
    while n:
        n, rem = divmod(n - 1, 26)
        letters = chr(65 + rem) + letters
    return letters

def ensure_columns(headers, required_headers):
    """
    Checks if required headers exist in the list. 
    Returns a new list with missing headers appended.
    """
    updated = list(headers)
    for c in required_headers:
        if c not in updated:
            updated.append(c)
    return updated
def get_days_diff(date_str: str) -> int:
    """
    Calculates the number of days passed since a given date string.
    Handles both ISO format (YYYY-MM-DDTHH:MM:SS) and simple (YYYY-MM-DD).
    Returns 999 if the date is invalid or empty (to treat as 'long ago').
    """
    if not date_str:
        return 999 
    try:
        # Clean the string (remove time if exists, take only YYYY-MM-DD)
        clean_date = date_str.split("T")[0].split(" ")[0]
        dt = datetime.strptime(clean_date, "%Y-%m-%d")
        return (datetime.now() - dt).days
    except Exception:
        return 999