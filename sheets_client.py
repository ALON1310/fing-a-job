import os
import json
import gspread
import streamlit as st
from google.oauth2.service_account import Credentials

def get_sheet_client():
    """
    Returns an authenticated gspread client.
    
    Authentication Logic Priority:
    1. Streamlit Cloud Secrets (st.secrets): Best for Streamlit Cloud deployment.
    2. Environment Variable (GCP_SERVICE_ACCOUNT): Best for GitHub Actions.
    3. Local File (credentials.json): Best for local development.
    """
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]

    # --- OPTION A: STREAMLIT SECRETS (Cloud) ---
    # Checks if running on Streamlit Cloud with configured secrets
    if "GCP_SERVICE_ACCOUNT" in st.secrets:
        try:
            # Convert Streamlit's internal object to a standard dictionary
            creds_dict = dict(st.secrets["GCP_SERVICE_ACCOUNT"])

            # CRITICAL FIX: Handle escaped newlines in the private key
            # This fixes the common copy-paste issue in Streamlit Secrets
            if "private_key" in creds_dict:
                creds_dict["private_key"] = creds_dict["private_key"].replace("\\n", "\n")

            creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
            return gspread.authorize(creds)
        except Exception as e:
            # Log error but allow falling back to other methods if needed
            print(f"⚠️ Error loading Streamlit Secrets: {e}")

    # --- OPTION B: ENVIRONMENT VARIABLE (GitHub Actions) ---
    # Checks for a raw JSON string injected via environment variable
    json_creds = os.getenv("GCP_SERVICE_ACCOUNT")
    if json_creds:
        try:
            creds_dict = json.loads(json_creds)
            creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
            return gspread.authorize(creds)
        except json.JSONDecodeError:
            pass # Not a valid JSON string, skip to next option

    # --- OPTION C: LOCAL FILE (Local Dev) ---
    # Checks for the physical credentials.json file on the disk
    creds_file = "credentials.json"
    if os.path.exists(creds_file):
        creds = Credentials.from_service_account_file(creds_file, scopes=scopes)
        return gspread.authorize(creds)

    # --- FAILURE ---
    raise RuntimeError(
        "❌ Authentication Failed: Could not find credentials in st.secrets (Cloud), "
        "GCP_SERVICE_ACCOUNT env var (GitHub), or credentials.json (Local)."
    )