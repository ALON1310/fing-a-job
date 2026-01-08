# sheets_client.py
import os
import gspread
from google.oauth2.service_account import Credentials

def get_sheet_client():
    """
    Returns an authenticated gspread client using a Google Service Account JSON file.

    Required env:
    - GOOGLE_CREDS_FILE: path to the service account JSON file (e.g. ./service_account.json)

    Notes:
    - The service account email must have access to your Google Sheet (share the sheet with it).
    """
    creds_file = os.getenv("GOOGLE_CREDS_FILE", "").strip()
    if not creds_file:
        raise RuntimeError("Missing GOOGLE_CREDS_FILE in environment. Point it to your service account JSON file.")

    if not os.path.exists(creds_file):
        raise RuntimeError(f"GOOGLE_CREDS_FILE not found: {creds_file}")

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]

    creds = Credentials.from_service_account_file(creds_file, scopes=scopes)
    return gspread.authorize(creds)
