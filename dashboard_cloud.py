import streamlit as st
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import time

# --- UI CONFIG ---
st.set_page_config(page_title="Master CRM (Cloud)", page_icon="☁️", layout="wide")
st.title("☁️ Master Sales CRM (Google Sheets)")

# --- CONNECTION FUNCTION ---
def get_gspread_client():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    
    # Attempt 1: Local loading (from your computer using credentials.json)
    try:
        creds = ServiceAccountCredentials.from_json_keyfile_name("credentials.json", scope)
        return gspread.authorize(creds)
    except Exception:
        pass
    
    # Attempt 2: Cloud loading (using Streamlit Secrets)
    try:
        key_dict = {
            "type": st.secrets["gcp_service_account"]["type"],
            "project_id": st.secrets["gcp_service_account"]["project_id"],
            "private_key_id": st.secrets["gcp_service_account"]["private_key_id"],
            "private_key": st.secrets["gcp_service_account"]["private_key"],
            "client_email": st.secrets["gcp_service_account"]["client_email"],
            "client_id": st.secrets["gcp_service_account"]["client_id"],
            "auth_uri": st.secrets["gcp_service_account"]["auth_uri"],
            "token_uri": st.secrets["gcp_service_account"]["token_uri"],
            "auth_provider_x509_cert_url": st.secrets["gcp_service_account"]["auth_provider_x509_cert_url"],
            "client_x509_cert_url": st.secrets["gcp_service_account"]["client_x509_cert_url"],
        }
        creds = ServiceAccountCredentials.from_json_keyfile_dict(key_dict, scope)
        return gspread.authorize(creds)
    except Exception:
        return None

# --- LOAD DATA ---
def load_data():
    client = get_gspread_client()
    if not client:
        st.error("🚨 Authentication Error: Could not find credentials.json or Streamlit Secrets.")
        st.stop()
        
    try:
        # Open the Google Sheet by name
        sheet = client.open("Master_Leads_DB").sheet1
        data = sheet.get_all_records()
        df = pd.DataFrame(data)
        return df, sheet
    except Exception as e:
        st.error(f"⚠️ Error connecting to Google Sheet: {e}")
        st.stop()

# --- MAIN APP ---
df, sheet_obj = load_data()

# Ensure critical columns exist
expected_cols = ["Job Title", "Salary", "Post Date", "Contact Info", "Link", "Description", "Status", "Sales Rep", "Notes"]
for col in expected_cols:
    if col not in df.columns:
        df[col] = ""

# --- METRICS ---
c1, c2, c3 = st.columns(3)
c1.metric("Total Leads", len(df))
if "Status" in df.columns:
    c2.metric("New Leads", len(df[df["Status"] == "New"]))
    c3.metric("Hot Leads 🔥", len(df[df["Status"] == "Hot Lead"]))

st.divider()

# --- FILTERS ---
with st.sidebar:
    st.header("🔍 Filters")
    status_filter = st.multiselect("Status:", options=df["Status"].unique(), default=df["Status"].unique())
    rep_filter = st.multiselect("Sales Rep:", options=df["Sales Rep"].unique(), default=df["Sales Rep"].unique())

filtered_df = df[
    (df["Status"].isin(status_filter)) & 
    (df["Sales Rep"].isin(rep_filter))
]

# --- EDITOR ---
st.info("💡 Data is synced directly with Google Sheets.")
edited_df = st.data_editor(
    filtered_df,
    num_rows="dynamic",
    use_container_width=True,
    height=600,
    column_config={
        "Link": st.column_config.LinkColumn("Link", display_text="Open"),
        "Status": st.column_config.SelectboxColumn("Status", options=["New", "In Progress", "Hot Lead", "Closed", "Not Relevant"], required=True),
        "Sales Rep": st.column_config.SelectboxColumn("Sales Rep", options=["Dor", "Alon", "Unassigned"]),
        "Description": st.column_config.TextColumn("Description", width="large"),
        "Contact Info": st.column_config.TextColumn("Contact Info", width="medium"),
    },
    hide_index=True
)

# --- SAVE BUTTON ---
if st.button("💾 Save to Google Sheets", type="primary"):
    try:
        # Warning: Overwrites the sheet with the current visible data
        if len(filtered_df) < len(df):
            st.warning("⚠️ Warning: You are filtering rows. Saving now might hide invisible rows. Clear filters first!")
        else:
            sheet_obj.clear()
            # Write headers
            sheet_obj.append_row(edited_df.columns.tolist())
            # Write data
            sheet_obj.append_rows(edited_df.values.tolist())
            
            st.toast("✅ Google Sheet Updated Successfully!", icon="☁️")
            time.sleep(1)
            st.rerun()
            
    except Exception as e:
        st.error(f"Save Failed: {e}")