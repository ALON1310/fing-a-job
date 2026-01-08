import streamlit as st
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import time
import os
from dotenv import load_dotenv

# --- CONFIG ---
st.set_page_config(page_title="Master CRM (Cloud)", page_icon="☁️", layout="wide")
st.title("☁️ Master Sales CRM (Google Sheets)")

# Load Env (local dev)
load_dotenv()

# --- CONNECTION FUNCTION ---
def get_gspread_client():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    
    # Option 1: Local File
    creds_file = os.getenv("GOOGLE_CREDS_FILE", "credentials.json") # Updated based on your .env
    if os.path.exists(creds_file):
        try:
            creds = ServiceAccountCredentials.from_json_keyfile_name(creds_file, scope)
            return gspread.authorize(creds)
        except Exception:
            pass
            
    # Option 2: Streamlit Secrets (Cloud)
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
        st.error("🚨 Authentication Error. Check credentials.")
        st.stop()
        
    sheet_name = os.getenv("SHEET", "Master_Leads_DB") # Matches your .env
    
    try:
        sheet = client.open(sheet_name).sheet1
        data = sheet.get_all_records()
        df = pd.DataFrame(data)
        
        # Try to load sales reps options (optional)
        try:
            settings_sheet = client.open(sheet_name).worksheet("Settings")
            reps_list = settings_sheet.col_values(1)
            sales_reps_options = [x for x in reps_list if x and x != "Sales Reps"]
        except gspread.WorksheetNotFound:
            sales_reps_options = ["Dor", "Alon", "Unassigned"]
            
        return df, sheet, sales_reps_options
    except Exception as e:
        st.error(f"⚠️ Error connecting to Google Sheet: {e}")
        st.stop()

# --- HELPER: PARSE DATE ---
def parse_date(date_str):
    if not isinstance(date_str, str):
        return pd.NaT
    try:
        clean = date_str.replace(",", "").strip()
        return pd.to_datetime(clean, format="%b %d %Y", errors='coerce')
    except Exception:
        return pd.NaT

# --- MAIN APP ---
df, sheet_obj, sales_reps_options = load_data()

# Ensure ALL columns exist (CRM + Automation) - Critical for saving!
all_cols = [
    "Job Title", "Salary", "Post Date", "Contact Info", "Link", "Description", 
    "Status", "Sales Rep", "Notes", 
    "Send Mode", "Send Status", "Send Attempts", "Last Error", "Last Sent At",
    "Draft Email", "Email Subject"
]

for col in all_cols:
    if col not in df.columns:
        df[col] = ""

# --- STATUS LIST ---
STATUS_OPTIONS = ["New", "In Progress", "Hot Lead", "Lost", "Not Relevant"]

# --- PRE-PROCESSING ---
df["_sort_date"] = df["Post Date"].apply(parse_date)

# --- SIDEBAR ---
with st.sidebar:
    st.header("🔍 Filters & Sorting")
    
    # Sort
    sort_cols_selection = st.multiselect(
        "Sort By:", 
        options=["Date", "Status", "Sales Rep", "Salary"],
        default=["Date"]
    )
    sort_ascending = st.checkbox("Ascending Order (A-Z)?", value=False)
    
    st.divider()
    
    # Status Filter
    status_filter = st.multiselect(
        "Filter Status:", 
        options=STATUS_OPTIONS,
        default=[] 
    )
    
    # Rep Filter
    existing_reps = df["Sales Rep"].unique().tolist() if "Sales Rep" in df.columns else []
    all_reps = list(set(sales_reps_options + existing_reps))
    rep_filter = st.multiselect("Filter Sales Rep:", options=all_reps, default=[])

# --- FILTERING LOGIC ---
# We work on a COPY for display, but keep original DF for saving
filtered_df = df.copy()

# 1. Status Logic
if not status_filter:
    filtered_df = filtered_df[~filtered_df["Status"].isin(["Lost", "Not Relevant"])]
else:
    filtered_df = filtered_df[filtered_df["Status"].isin(status_filter)]

# 2. Rep Logic
if rep_filter:
    filtered_df = filtered_df[filtered_df["Sales Rep"].isin(rep_filter)]

# 3. Sorting Logic
if sort_cols_selection:
    col_map = {"Date": "_sort_date", "Status": "Status", "Sales Rep": "Sales Rep", "Salary": "Salary"}
    actual_sort_cols = [col_map[c] for c in sort_cols_selection]
    filtered_df = filtered_df.sort_values(by=actual_sort_cols, ascending=sort_ascending)

# --- DISPLAY ---
# Define columns to SHOW in the editor (Hide technical automation columns)
display_columns = [
    "Job Title", "Salary", "Post Date", "Contact Info", "Link", 
    "Description", "Status", "Sales Rep", "Notes", "Draft Email", "Send Status"
]

# Create a view for the editor
editor_view = filtered_df[display_columns].copy()

# --- METRICS ---
# Here we added the 5th column for Manual Check
c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Active Leads", len(filtered_df))
c2.metric("New Leads", len(df[df["Status"] == "New"]))
c3.metric("Hot Leads 🔥", len(df[df["Status"] == "Hot Lead"]))
c4.metric("Sent ✉️", len(df[df["Send Status"] == "SENT"]))

# This is the new part:
manual_count = len(df[df["Send Status"] == "MANUAL_CHECK"])
c5.metric("Manual Check ✋", manual_count)

st.divider()

# --- EDITOR ---
st.info(f"💡 Showing {len(editor_view)} leads.")

edited_view = st.data_editor(
    editor_view,
    num_rows="dynamic",
    use_container_width=True,
    height=600,
    column_config={
        "Link": st.column_config.LinkColumn("Link", display_text="Open"),
        "Status": st.column_config.SelectboxColumn("Status", options=STATUS_OPTIONS, required=True),
        "Sales Rep": st.column_config.SelectboxColumn("Sales Rep", options=sales_reps_options),
        "Salary": st.column_config.TextColumn("Salary 💰", width="medium"),
        "Description": st.column_config.TextColumn("Description", width="large"),
        "Contact Info": st.column_config.TextColumn("Contact Info", width="medium"),
        "Draft Email": st.column_config.TextColumn("Draft Email", width="large"),
        "Send Status": st.column_config.TextColumn("Send Status", disabled=True), # Read Only
    },
    hide_index=True
)

# --- SAVE LOGIC (SMART MERGE) ---
st.divider()

if st.button("💾 Save to Google Sheets", type="primary"):
    try:
        with st.spinner("Saving changes..."):
            # Update the original DF with changes from the editor
            # This logic preserves the hidden columns
            df.update(edited_view)
            
            # Prepare for upload (drop temporary sort col)
            final_df = df.drop(columns=["_sort_date"], errors="ignore")
            
            # Reorder columns to match Sheet structure exactly
            final_df = final_df[all_cols]
            
            # Upload
            sheet_obj.clear()
            sheet_obj.update([final_df.columns.values.tolist()] + final_df.values.tolist())
        
        st.toast("✅ Google Sheet Updated Successfully!", icon="☁️")
        time.sleep(1)
        st.rerun()
            
    except Exception as e:
        st.error(f"❌ Save Failed: {e}")