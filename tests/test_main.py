from unittest.mock import patch
import sys
import os

# Import the main entry point
from main import main

@patch("main.subprocess.run")
@patch("main.os.system")  # Mock os.system to prevent clearing the actual terminal screen
@patch("builtins.input")  # Mock user input to simulate CLI interactions
def test_main_menu_scraper_option(mock_input, mock_system, mock_subprocess):
    """
    Scenario: User selects option '1' (Scraper) followed by 'q' (Quit).
    Verification: Ensures 'scraper_agent.py' is executed via subprocess.
    """
    # Input simulation sequence:
    # '1' -> Select Scraper option
    # ''  -> Press Enter to continue (after script execution)
    # 'q' -> Quit application
    mock_input.side_effect = ['1', '', 'q']
    
    main()
    
    # Verify the correct script was called using the current Python interpreter
    mock_subprocess.assert_called_with([sys.executable, "scraper_agent.py"], check=True)

@patch("main.subprocess.run")
@patch("main.os.system")
@patch("builtins.input")
def test_main_menu_dry_run_option(mock_input, mock_system, mock_subprocess):
    """
    Scenario: User selects option '2' (Dry Run).
    Verification: Environment variables are set to DRYRUN mode and 'sender_agent.py' is executed.
    """
    mock_input.side_effect = ['2', '', 'q']
    
    # Run within a clean environment context
    with patch.dict(os.environ, {}, clear=True):
        main()
        
        # Verify Environment Variables were set correctly
        assert os.environ.get("MODE") == "DRYRUN"
        assert os.environ.get("VERIFY_ONLY") == "0"
        
    mock_subprocess.assert_called_with([sys.executable, "sender_agent.py"], check=True)

@patch("main.subprocess.run")
@patch("main.os.system")
@patch("builtins.input")
def test_main_menu_real_email_confirmed(mock_input, mock_system, mock_subprocess):
    """
    Scenario: User selects option '5' (Real Email) and confirms with 'YES'.
    Verification: Environment mode changes to REAL and 'sender_agent.py' is executed.
    """
    # Input simulation: Select 5 -> Confirm 'YES' -> Press Enter -> Quit
    mock_input.side_effect = ['5', 'YES', '', 'q']
    
    with patch.dict(os.environ, {}, clear=True):
        main()
        assert os.environ.get("MODE") == "REAL"
    
    mock_subprocess.assert_called_with([sys.executable, "sender_agent.py"], check=True)

@patch("main.subprocess.run")
@patch("main.os.system")
@patch("builtins.input")
def test_main_menu_real_email_aborted(mock_input, mock_system, mock_subprocess):
    """
    Scenario: User selects option '5' (Real Email) but types 'NO' at confirmation.
    Verification: The script execution is aborted (subprocess is not called).
    """
    # Input simulation: Select 5 -> Deny 'NO' -> Quit
    # Note: No 'Enter' input needed because the script doesn't run
    mock_input.side_effect = ['5', 'NO', 'q']
    
    with patch("main.time.sleep"):  # Skip sleep delay to speed up test
        main()
    
    # Verify that the subprocess was NEVER called
    mock_subprocess.assert_not_called()

@patch("main.subprocess.run")
@patch("main.os.system")
@patch("builtins.input")
def test_dashboard_launch(mock_input, mock_system, mock_subprocess):
    """
    Scenario: User selects option '4' (Dashboard).
    Verification: The 'streamlit run' command is triggered.
    """
    mock_input.side_effect = ['4', 'q']
    
    main()
    
    # Note: Streamlit command is passed as a list of strings, not using sys.executable
    mock_subprocess.assert_called_with(["streamlit", "run", "dashboard.py"])