import pytest
from unittest.mock import MagicMock, patch
import os
import json

from scraper_agent import (
    canonical_job_url,
    normalize_contact,
    has_real_contact,
    extract_data_with_ai,
    generate_email_body,
    run_job_seeker_agent
)
import salary_parser

# ---------------------------------------------------------
# Unit Tests
# ---------------------------------------------------------

def test_canonical_job_url():
    """Verify that job URLs are normalized correctly (ID extraction)."""
    raw = "https://www.onlinejobs.ph/jobseekers/job/12345/Dev"
    expected = "https://www.onlinejobs.ph/jobseekers/job/12345"
    assert canonical_job_url(raw) == expected

def test_is_salary_too_low():
    """Verify salary filtering logic."""
    assert salary_parser.is_salary_too_low("400/mo", 900, 50000) is True
    assert salary_parser.is_salary_too_low("$1000", 900, 50000) is False   # Acceptable

def test_has_real_contact():
    """Verify contact info detection."""
    assert has_real_contact("alon@test.com") is True
    assert has_real_contact("None") is False

def test_normalize_contact():
    """Verify contact info normalization and cleaning."""
    assert normalize_contact({"email": "a@b.com"}) == "a@b.com"
    assert normalize_contact(None) == "None"

def test_generate_email_body():
    """Verify email body generation contains expected placeholders."""
    body = generate_email_body("Alon", "Dev", "Hook")
    assert "Hi Alon" in body

# ---------------------------------------------------------
# Integration Tests
# ---------------------------------------------------------

@pytest.fixture
def mock_dependencies():
    """
    Setup complex mocks for OpenAI, Playwright, and Google Sheets.
    This fixture ensures no actual external calls are made during tests.
    """
    with patch('scraper_agent.OpenAI') as mock_openai_cls, \
         patch('scraper_agent.sync_playwright') as mock_playwright, \
         patch('scraper_agent.get_sheet_client') as mock_sheets, \
         patch('scraper_agent.get_existing_links', return_value=set()):
        
        # 1. Setup OpenAI Mock
        mock_client_instance = MagicMock()
        mock_completion = MagicMock()
        mock_choice = MagicMock()
        
        # Simulate a valid JSON response from GPT
        mock_choice.message.content = json.dumps({
            "contact": "test@gmail.com", 
            "name": "Dave", 
            "hook": "Nice job"
        })
        mock_completion.choices = [mock_choice]
        mock_client_instance.chat.completions.create.return_value = mock_completion
        mock_openai_cls.return_value = mock_client_instance

        # 2. Setup Playwright Mocks
        mock_p_ctx = MagicMock()
        mock_playwright.return_value.__enter__.return_value = mock_p_ctx
        
        mock_browser = MagicMock()
        mock_context = MagicMock()
        mock_page = MagicMock() 
        
        mock_p_ctx.chromium.launch.return_value = mock_browser
        mock_browser.new_context.return_value = mock_context
        # Critical: Ensure new_page() always returns the SAME mock object
        # This allows us to assert actions performed on "new pages" easily.
        mock_context.new_page.return_value = mock_page 
        
        yield {
            "page": mock_page,
            "context": mock_context,
            "openai": mock_client_instance,
            "sheets": mock_sheets
        }

def test_extract_data_with_ai(mock_dependencies):
    """Test AI extraction logic with a mocked OpenAI client."""
    with patch('scraper_agent.client', mock_dependencies["openai"]):
        # Use a long string to bypass internal length validation checks
        long_desc = "This is a very long job description that should definitely pass the length check."
        result = extract_data_with_ai(long_desc, "Dev Job")
        assert result["contact"] == "test@gmail.com"

def test_run_scraper_flow(mock_dependencies):
    """
    End-to-End test of the scraper flow.
    Simulates: Login -> Navigation -> Job Finding -> Data Extraction -> Saving to Sheets.
    """
    mocks = mock_dependencies
    main_page = mocks["page"]
    
    # 1. Prepare a mock job link element
    mock_link_element = MagicMock()
    mock_link_element.get_attribute.return_value = "/jobseekers/job/99999"

    # 2. Define Dynamic Locator Behavior (Side Effect)
    # This function mocks the browser's response depending on the CSS selector used.
    def universal_locator_side_effect(selector):
        m = MagicMock()
        
        # -- Job Links List --
        if 'div.desc a' in selector:
            m.all.return_value = [mock_link_element]
            return m

        # -- Login Page Fields --
        if "login_username" in selector or "login_password" in selector:
            m.count.return_value = 1
        elif "submit" in selector:
            m.count.return_value = 1
            
        # -- Job Description Page --
        elif "#job-description" in selector:
            m.count.return_value = 1
            m.inner_text.return_value = "Long description... contact: test@gmail.com"
        elif "h1" in selector:  # Job Title
            m.count.return_value = 1
            m.inner_text.return_value = "Python Developer"
        elif "dl > dd > p" in selector:  # Salary
            m.count.return_value = 1
            # Mock both direct inner_text and .first.inner_text to be safe
            m.inner_text.return_value = "$2000"
            m.first.inner_text.return_value = "$2000"
            
        # -- Navigation & Popups --
        elif "button" in selector: 
             m.count.return_value = 0
        elif "pagination" in selector: 
             m.count.return_value = 0  # Return 0 to stop the pagination loop
             m.is_visible.return_value = False
        else:
            m.count.return_value = 0
            m.inner_text.return_value = ""
        
        # Default: .first returns self to allow chaining
        m.first = m 
        return m

    # 3. Apply the side effect to the page locator
    main_page.locator.side_effect = universal_locator_side_effect

    # 4. Run the Agent
    # We set BATCH_SIZE to '1' to force an immediate save to Sheets after finding one job.
    env_vars = {
        "OJ_EMAIL": "u", 
        "OJ_PASSWORD": "p", 
        "SHEET": "S",
        "BATCH_SIZE": "1"
    }
    
    with patch.dict(os.environ, env_vars):
        with patch('scraper_agent.client', mocks["openai"]):
            run_job_seeker_agent()

    # 5. Assertions
    
    # Verify Login: Check if username field was filled
    login_calls = [str(c) for c in main_page.fill.call_args_list]
    assert any("login_username" in c for c in login_calls), "Login process was skipped!"

    # Verify Save: Check if data was appended to Google Sheets
    mock_sheet = mocks["sheets"].return_value.open.return_value.sheet1
    mock_sheet.append_rows.assert_called()