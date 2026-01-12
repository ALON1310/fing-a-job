import os
import json
import gspread
from google.oauth2.service_account import Credentials

def get_sheet_client():
    """
    Returns an authenticated gspread client.
    Smart Logic:
    1. Checks for 'GCP_SERVICE_ACCOUNT' env var (Cloud/GitHub Actions mode).
    2. Falls back to 'credentials.json' file (Local mode).
    """
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]

    # --- OPTION A: CLOUD (GitHub Secrets) ---
    # Check if the JSON is stored in an environment variable
    json_creds = os.getenv("GCP_SERVICE_ACCOUNT")
    
    if json_creds:
        try:
            # Parse the JSON string into a dictionary
            creds_dict = json.loads(json_creds)
            creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
            return gspread.authorize(creds)
        except json.JSONDecodeError as e:
            print(f"⚠️ Error parsing GCP_SERVICE_ACCOUNT: {e}")
            # If parsing fails, fall back to Option B
            pass

    # --- OPTION B: LOCAL FILE ---
    # Check for a physical file on the disk
    creds_file = os.getenv("GOOGLE_CREDS_FILE", "credentials.json")

    if os.path.exists(creds_file):
        creds = Credentials.from_service_account_file(creds_file, scopes=scopes)
        return gspread.authorize(creds)

    # If both options fail:
    raise RuntimeError(
        "❌ Authentication Failed: Could not find 'GCP_SERVICE_ACCOUNT' secret (Cloud) "
        "OR 'credentials.json' file (Local)."
    )