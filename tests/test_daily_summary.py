import pytest
from unittest.mock import MagicMock, patch
from daily_summary import send_daily_summary

# ---------------------------------------------------------
# Custom Mocks (Robust datetime mocking)
# ---------------------------------------------------------

class MockNowObj:
    """A fake datetime object that always returns a specific string for strftime."""
    def strftime(self, fmt):
        return "Jan 20 2026"

class MockDatetime:
    """A fake datetime class that replaces the real 'datetime'."""
    @classmethod
    def now(cls):
        return MockNowObj()

# ---------------------------------------------------------
# Fixtures
# ---------------------------------------------------------

@pytest.fixture
def mock_env():
    """
    Sets up the mocked environment:
    1. Mocks Google Sheets client.
    2. Mocks SMTP.
    3. Mocks datetime (Uses the class above).
    """
    # Patch the 'datetime' imported inside daily_summary.py
    with patch("daily_summary.get_sheet_client") as mock_client, \
         patch("daily_summary.smtplib.SMTP") as mock_smtp, \
         patch("daily_summary.datetime", new=MockDatetime): 
        
        # 1. Setup Sheet Mock
        mock_ws = MagicMock()
        mock_client.return_value.open.return_value.sheet1 = mock_ws
        
        # 2. Setup SMTP Mock
        smtp_instance = MagicMock()
        mock_smtp.return_value.__enter__.return_value = smtp_instance
        
        yield {
            "ws": mock_ws,
            "smtp": smtp_instance
        }

# ---------------------------------------------------------
# Helpers
# ---------------------------------------------------------

def get_email_text_body(smtp_mock):
    """
    Helper to extract the PLAIN TEXT body from a multipart email object.
    """
    args, _ = smtp_mock.send_message.call_args
    sent_msg = args[0]
    
    # Since the email is multipart (Text + HTML), we must ask specifically for 'plain'
    # .get_body(preferencelist=('plain')) returns the text part object
    # .get_content() then reads the string inside it
    return sent_msg.get_body(preferencelist=('plain')).get_content()

# ---------------------------------------------------------
# Test Cases
# ---------------------------------------------------------

def test_daily_summary_happy_flow(mock_env):
    """
    Scenario: Standard day. 2 leads from today (Jan 20 2026).
    Expectation: Email sent with Total=2.
    """
    ws = mock_env["ws"]
    smtp = mock_env["smtp"]
    
    # Mock Data
    records = [
        {"Post Date": "Jan 19 2026", "Contact Info": "ignore@test.com", "Job Title": "Old"},
        {"Post Date": "Jan 20 2026", "Contact Info": "jane@email.com", "Job Title": "New 1"}, # Email
        {"Post Date": "Jan 20 2026", "Contact Info": "050-1234567", "Job Title": "New 2"}     # Phone
    ]
    ws.get_all_records.return_value = records
    
    send_daily_summary()
    
    # Assert
    smtp.send_message.assert_called_once()
    
    # Extract body using the new helper
    body = get_email_text_body(smtp)
    
    assert "Total New Leads Found: 2" in body
    assert "1 leads have phone numbers" in body
    assert "1 leads have no phone" in body

def test_daily_summary_no_leads_today(mock_env):
    """
    Scenario: Data exists, but dates don't match today.
    Expectation: Email sent with 0 new leads.
    """
    ws = mock_env["ws"]
    smtp = mock_env["smtp"]
    
    records = [
        {"Post Date": "Jan 10 2026", "Contact Info": "old@test.com"}
    ]
    ws.get_all_records.return_value = records
    
    send_daily_summary()
    
    smtp.send_message.assert_called_once()
    
    body = get_email_text_body(smtp)
    assert "Total New Leads Found: 0" in body

def test_daily_summary_empty_sheet(mock_env):
    """
    Scenario: Sheet is completely empty.
    Expectation: No email sent.
    """
    ws = mock_env["ws"]
    smtp = mock_env["smtp"]
    
    ws.get_all_records.return_value = [] 
    
    send_daily_summary()
    
    smtp.send_message.assert_not_called()

def test_daily_summary_missing_columns(mock_env):
    """
    Scenario: 'Post Date' column is missing.
    Expectation: Code falls back to taking the last 20 rows.
    """
    ws = mock_env["ws"]
    smtp = mock_env["smtp"]
    
    # Create 25 rows without "Post Date"
    records = [{"Contact Info": "test@test.com", "Job Title": "T"} for _ in range(25)]
    ws.get_all_records.return_value = records
    
    send_daily_summary()
    
    smtp.send_message.assert_called_once()
    
    body = get_email_text_body(smtp)
    # Logic fallback: takes tail(20)
    assert "Total New Leads Found: 20" in body

def test_daily_summary_smtp_failure(mock_env):
    """
    Scenario: SMTP crashes.
    Expectation: Graceful handling (no crash).
    """
    ws = mock_env["ws"]
    smtp = mock_env["smtp"]
    
    records = [{"Post Date": "Jan 20 2026", "Contact Info": "a@b.com"}]
    ws.get_all_records.return_value = records
    
    smtp.send_message.side_effect = Exception("SMTP Crash")
    
    try:
        send_daily_summary()
    except Exception:
        pytest.fail("Function crashed instead of catching exception")
        
    smtp.send_message.assert_called_once()

def test_date_format_variations(mock_env):
    """
    Scenario: 'Post Date' has extra spaces.
    Expectation: Contains logic matches it.
    """
    ws = mock_env["ws"]
    smtp = mock_env["smtp"]
    
    records = [
        {"Post Date": "Jan 20 2026 ", "Contact Info": "space@test.com"}, 
        {"Post Date": "Date: Jan 20 2026", "Contact Info": "prefix@test.com"} 
    ]
    ws.get_all_records.return_value = records
    
    send_daily_summary()
    
    body = get_email_text_body(smtp)
    assert "Total New Leads Found: 2" in body