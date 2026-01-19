import pytest
from unittest.mock import MagicMock, patch

# Import the module and functions under test
from sender_agent import (
    run_sender_agent,
    get_cell,
    set_cell,
    default_subject_from_row,
    resolve_body
)

# ---------------------------------------------------------
# Unit Tests (Helper Functions)
# Testing small logic functions without external dependencies
# ---------------------------------------------------------

def test_get_and_set_cell():
    """Verifies the helper functions for reading and writing row data."""
    headers = ["Name", "Email", "Status"]
    row = ["Alon", "alon@test.com", "New"]
    
    # Test Reading
    assert get_cell(row, headers, "Name") == "Alon"
    assert get_cell(row, headers, "Email") == "alon@test.com"
    
    # Test Writing
    set_cell(row, headers, "Status", "Sent")
    assert row[2] == "Sent"
    
    # Test Writing to a new column (Should extend the row)
    headers.append("Notes")
    set_cell(row, headers, "Notes", "Test Note")
    assert len(row) == 4
    assert row[3] == "Test Note"

def test_default_subject():
    """Verifies the default subject generation based on Job Title."""
    headers = ["Job Title"]
    row = ["Python Developer"]
    subject = default_subject_from_row(headers, row)
    assert "Python Developer" in subject

def test_resolve_body_draft():
    """Verifies that the Draft Email content is correctly resolved."""
    headers = ["Draft Email"]
    row = ["Hi Alon, this is a draft."]
    
    body, source = resolve_body(headers, row)
    assert body == "Hi Alon, this is a draft."
    assert source == "Draft Email"

# ---------------------------------------------------------
# Integration Tests with Mocking
# ---------------------------------------------------------

@pytest.fixture
def mock_env():
    """
    Sets up the mocked infrastructure (SMTP, Google Sheets) 
    to prevent actual network calls during tests.
    """
    with patch("sender_agent.get_sheet_client") as mock_client, \
         patch("sender_agent.smtplib.SMTP") as mock_smtp, \
         patch("sender_agent.time.sleep"), \
         patch("sender_agent.get_timestamp_iso", return_value="2025-01-01T12:00:00"):
        
        # 1. Mock Google Sheets
        mock_ws = MagicMock()
        mock_client.return_value.open.return_value.sheet1 = mock_ws
        
        # 2. Mock SMTP (Prevent actual email sending)
        smtp_instance = MagicMock()
        mock_smtp.return_value.__enter__.return_value = smtp_instance
        
        yield {
            "ws": mock_ws,
            "smtp": smtp_instance
        }

def test_run_sender_agent_real_mode(mock_env):
    """
    Scenario: REAL mode.
    Expectation: Email is sent via SMTP, and Sheet is updated to SENT and Follow-up.
    """
    mocks = mock_env
    ws = mocks["ws"]
    smtp = mocks["smtp"]

    # Prepare Mock Data
    headers = [
        "Status", "Contact Info", "Draft Email", "Email Subject", 
        "Send Status", "Followup Count", "Last Sent At", "Send Mode", "Send Attempts", "Last Error"
    ]
    # Row representing a new lead
    row_data = ["New", "alon@test.com", "Hi body", "Subject", "", "0", "", "", "", ""]
    
    ws.get_all_values.return_value = [headers, row_data]

    # Run in REAL mode
    with patch("sender_agent.MODE", "REAL"):
        # Mock missing SMTP credentials for the test context
        with patch("sender_agent.SMTP_USER", "user"), patch("sender_agent.SMTP_PASS", "pass"):
            run_sender_agent()

    # 1. Verify Email Sent
    smtp.send_message.assert_called_once()
    
    # 2. Verify Sheet Update
    assert ws.batch_update.called
    
    # Inspect the batch_update payload to ensure correct data values
    args, _ = ws.batch_update.call_args
    batch_payload = args[0]  # The list of updates sent to Sheets
    updated_row = batch_payload[0]['values'][0]  # The actual updated row
    
    # Verify critical values
    assert get_cell(updated_row, headers, "Send Status") == "SENT"
    assert get_cell(updated_row, headers, "Status") == "Follow-up"      # Status changed
    assert get_cell(updated_row, headers, "Followup Count") == "1"      # Count incremented
    assert get_cell(updated_row, headers, "Last Sent At") == "2025-01-01T12:00:00"

def test_run_sender_agent_dry_run(mock_env):
    """
    Scenario: DRYRUN mode.
    Expectation: No email is sent, Status updates to PENDING.
    """
    mocks = mock_env
    ws = mocks["ws"]
    smtp = mocks["smtp"]

    headers = ["Status", "Contact Info", "Draft Email", "Email Subject", "Send Status", "Followup Count", "Last Sent At", "Send Mode", "Send Attempts", "Last Error"]
    row_data = ["New", "alon@test.com", "Hi body", "Subject", "", "0", "", "", "", ""]
    ws.get_all_values.return_value = [headers, row_data]

    # Run in DRYRUN mode
    with patch("sender_agent.MODE", "DRYRUN"):
         run_sender_agent()

    # 1. Verify NO email sent
    smtp.send_message.assert_not_called()
    
    # 2. Verify Status is PENDING
    args, _ = ws.batch_update.call_args
    updated_row = args[0][0]['values'][0]
    assert get_cell(updated_row, headers, "Send Status") == "PENDING"

def test_run_sender_agent_skip_invalid_status(mock_env):
    """
    Scenario: Sheet Status is not 'New' (e.g., 'Closed').
    Expectation: The system SKIPS this row.
    """
    mocks = mock_env
    ws = mocks["ws"]

    headers = ["Status", "Contact Info", "Draft Email", "Email Subject", "Send Status", "Followup Count", "Last Sent At", "Send Mode", "Send Attempts", "Last Error"]
    # Row with invalid status
    row_data = ["Closed", "alon@test.com", "Hi", "Sub", "", "", "", "", "", ""]
    ws.get_all_values.return_value = [headers, row_data]

    run_sender_agent()

    # Verify Skipped
    args, _ = ws.batch_update.call_args
    updated_row = args[0][0]['values'][0]
    assert get_cell(updated_row, headers, "Send Status") == "SKIPPED"
    assert "Skipped due to Status" in get_cell(updated_row, headers, "Last Error")

def test_run_sender_agent_missing_draft(mock_env):
    """
    Scenario: Email exists, but 'Draft Email' column is empty.
    Expectation: Status updates to FAILED.
    """
    mocks = mock_env
    ws = mocks["ws"]

    headers = ["Status", "Contact Info", "Draft Email", "Email Subject", "Send Status", "Last Error"]
    # Row with empty draft body
    row_data = ["New", "alon@test.com", "", "Sub", "", ""] 
    ws.get_all_values.return_value = [headers, row_data]

    run_sender_agent()

    args, _ = ws.batch_update.call_args
    updated_row = args[0][0]['values'][0]
    assert get_cell(updated_row, headers, "Send Status") == "FAILED"
    assert "Missing Draft" in get_cell(updated_row, headers, "Last Error")