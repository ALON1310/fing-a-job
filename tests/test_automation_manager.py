import pytest
from unittest.mock import MagicMock, patch

# Import the function to be tested
from automation_manager import process_daily_automation

# --- Fixtures ---

@pytest.fixture
def mock_dependencies():
    """
    Creates mocks for Google Sheets, Email sending, and Time calculations.
    This ensures tests run without external API calls.
    """
    with patch('automation_manager.get_sheet_client') as mock_client, \
         patch('automation_manager.send_real') as mock_send, \
         patch('automation_manager.extract_email', side_effect=lambda x: x), \
         patch('automation_manager.get_days_diff') as mock_days, \
         patch('automation_manager.get_timestamp_iso', return_value="2025-01-01T10:00:00"):
        
        # Setup Mock Sheet Objects
        mock_ws = MagicMock()
        mock_doc = MagicMock()
        
        mock_client.return_value = mock_doc
        mock_doc.open.return_value = mock_doc
        mock_doc.sheet1 = mock_ws
        
        # Define headers so that column index lookups work correctly within the logic
        mock_ws.row_values.return_value = [
            "Name", "Status", "Contact Info", "Last Sent At", 
            "Followup Count", "Send Status", "Email Subject"
        ]
        
        yield {
            "ws": mock_ws,
            "send": mock_send,
            "days_diff": mock_days,
            "doc": mock_doc
        }

# --- Tests ---

def test_send_followup_success(mock_dependencies):
    """
    Scenario: Lead is ready for follow-up (enough days passed).
    Expected: Email sent and Sheet updated.
    """
    mocks = mock_dependencies
    
    fake_data = [{
        "Name": "Alon",
        "Status": "Follow-up",
        "Contact Info": "alon@test.com",
        "Last Sent At": "2024-01-01",
        "Followup Count": 2, 
        "Email Subject": "Dev Job"
    }]
    mocks["ws"].get_all_records.return_value = fake_data
    mocks["days_diff"].return_value = 8  # 8 days > 7 days threshold
    
    # Patch the global MODE variable to 'REAL' to enable actual sending logic
    with patch("automation_manager.MODE", "REAL"):
        process_daily_automation()
    
    # Assertions
    mocks["send"].assert_called_once()
    args, _ = mocks["send"].call_args
    assert args[0] == "alon@test.com"
    assert "re: dev job" in args[1].lower()
    
    # Verify Sheet updates (Date, Count, and Status)
    assert mocks["ws"].update_cell.call_count == 3 

def test_archive_lost_lead(mock_dependencies):
    """
    Scenario: Lead reached max follow-ups.
    Expected: Moved to 'Lost_Leads' tab and deleted from main sheet.
    """
    mocks = mock_dependencies
    
    fake_data = [{
        "Name": "Ghost",
        "Status": "Follow-up",
        "Contact Info": "ghost@test.com",
        "Last Sent At": "2024-01-01",
        "Followup Count": 5, # Max reached
    }]
    mocks["ws"].get_all_records.return_value = fake_data
    mocks["days_diff"].return_value = 4 
    
    mock_lost_sheet = MagicMock()
    mocks["doc"].worksheet.return_value = mock_lost_sheet

    # Patch MODE to REAL to allow archiving
    with patch("automation_manager.MODE", "REAL"):
        process_daily_automation()

    # Verify archiving logic
    mock_lost_sheet.append_rows.assert_called_once()
    mocks["ws"].delete_rows.assert_called_once()

def test_dry_run_does_nothing(mock_dependencies):
    """
    Scenario: MODE is DRYRUN.
    Expected: No emails sent, no database updates.
    """
    mocks = mock_dependencies
    
    fake_data = [{
        "Name": "Test",
        "Status": "Follow-up",
        "Contact Info": "test@test.com",
        "Last Sent At": "2024-01-01",
        "Followup Count": 2,
    }]
    mocks["ws"].get_all_records.return_value = fake_data
    mocks["days_diff"].return_value = 10 # Should send if REAL

    # No need to patch MODE here; default is DRYRUN
    process_daily_automation()

    # Verify strict inaction
    mocks["send"].assert_not_called()
    mocks["ws"].update_cell.assert_not_called()