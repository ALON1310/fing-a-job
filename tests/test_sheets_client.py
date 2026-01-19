import pytest
from unittest.mock import patch, ANY 
import os
import json

from sheets_client import get_sheet_client

@pytest.fixture
def mock_google_auth():
    with patch("sheets_client.Credentials") as mock_creds_cls, \
         patch("sheets_client.gspread.authorize") as mock_authorize, \
         patch("os.path.exists") as mock_exists:
        
        yield {
            "creds_cls": mock_creds_cls,
            "authorize": mock_authorize,
            "exists": mock_exists
        }

def test_auth_cloud_mode_success(mock_google_auth):
    mocks = mock_google_auth
    fake_creds_json = json.dumps({"type": "service_account", "project_id": "test"})
    
    with patch.dict(os.environ, {"GCP_SERVICE_ACCOUNT": fake_creds_json}):
        get_sheet_client()
        
    mocks["creds_cls"].from_service_account_info.assert_called_once()
    mocks["authorize"].assert_called_once()

def test_auth_local_mode_success(mock_google_auth):
    mocks = mock_google_auth
    with patch.dict(os.environ, {}, clear=True):
        mocks["exists"].return_value = True
        get_sheet_client()
    
    # שימוש ב-ANY כדי להתעלם מתוכן הרשימה של scopes
    mocks["creds_cls"].from_service_account_file.assert_called_once_with(
        "credentials.json", scopes=ANY
    )

def test_auth_fallback_on_bad_json(mock_google_auth):
    mocks = mock_google_auth
    bad_json = "{ I am not valid json }"
    
    with patch.dict(os.environ, {"GCP_SERVICE_ACCOUNT": bad_json}):
        mocks["exists"].return_value = True
        get_sheet_client()
        
    mocks["creds_cls"].from_service_account_file.assert_called_once()

def test_auth_failure_missing_everything(mock_google_auth):
    mocks = mock_google_auth
    with patch.dict(os.environ, {}, clear=True):
        mocks["exists"].return_value = False
        with pytest.raises(RuntimeError):
            get_sheet_client()