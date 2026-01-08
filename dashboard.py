import streamlit as st
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import time
import os
from dotenv import load_dotenv

# ---------------------------------------------------------
# 1. CONFIGURATION & SETUP
# ---------------------------------------------------------

# Page configuration
st.set_page_config(page_title="Platonics CRM", page_icon="☁️", layout="wide")
st.title("☁️ Platonics Lead Manager")

# Load environment variables (for local development)
load_dotenv()

# ---------------------------------------------------------
# 2. AUTHENTICATION (HYBRID: LOCAL + CLOUD)
# ---------------------------------------------------------

def get_gspread_client():
    """
    Authenticates with Google Sheets.
    - Tries local 'credentials.json' first.
    - Falls back to 'st.secrets' (Streamlit Cloud) if local file is missing.
    """
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    
    # Option A: Local File
    # (Matches the standard env var or defaults to current directory)
    creds_file = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "credentials.json")
    
    if os.path.exists(creds_file):
        try:
            creds = ServiceAccountCredentials.from_json_keyfile_name(creds_file, scope)
            return gspread.authorize(creds)
        except Exception:
            pass # Fail silently and try option B
            
    # Option B: Streamlit Cloud Secrets
    # (Used when deploying to share.streamlit.io)
    try:
        if "gcp_service_account" in st.secrets:
            key_dict = dict(st.secrets["gcp_service_account"])
            creds = ServiceAccountCredentials.from_json_keyfile_dict(key_dict, scope)
            return gspread.authorize(creds)
    except Exception:
        pass

    return None

# ---------------------------------------------------------
# 3. DATA LOADING
# ---------------------------------------------------------

def load_data():
    """
    Connects to the Sheet and downloads data into a Pandas DataFrame.
    Also fetches Sales Rep options if available.
    """
    client = get_gspread_client()
    if not client:
        st.error("🚨 Authentication Error. Please ensure 'credentials.json' exists locally or Secrets are configured in Cloud.")
        st.stop()
        
    sheet_name = os.getenv("SHEET", "Master_Leads_DB")
    
    try:
        # Open the spreadsheet
        main_doc = client.open(sheet_name)
        
        # Select the first sheet (Active Leads)
        sheet = main_doc.sheet1
        data = sheet.get_all_records()
        df = pd.DataFrame(data)
        
        # Attempt to load Sales Reps from a 'Settings' tab (Optional feature)
        try:
            # Placeholder: If you add a settings tab later, uncomment this:
            # settings_sheet = main_doc.worksheet("Settings")
            # reps_list = settings_sheet.col_values(1)
            # sales_reps_options = [x for x in reps_list if x and x != "Sales Reps"]
            sales_reps_options = ["Dor", "Alon", "Unassigned"]
        except gspread.WorksheetNotFound:
            sales_reps_options = ["Dor", "Alon", "Unassigned"]
            
        return df, sheet, main_doc, sales_reps_options
    except Exception as e:
        st.error(f"⚠️ Error connecting to Google Sheet '{sheet_name}': {e}")
        st.stop()

# Helper: Parse Dates safely
def parse_date(date_str):
    if not isinstance(date_str, str):
        return pd.NaT
    try:
        # Clean string "Jan 01 2026" -> datetime object
        clean = date_str.replace(",", "").strip()
        return pd.to_datetime(clean, format="%b %d %Y", errors='coerce')
    except Exception:
        return pd.NaT

# ---------------------------------------------------------
# 4. APP LOGIC
# ---------------------------------------------------------

# Load Data
df, sheet_obj, main_doc_obj, sales_reps_options = load_data()

# Define ALL columns (Critical for keeping structure when saving)
all_cols = [
    "Job Title", "Salary", "Post Date", "Contact Info", "Link", "Description", 
    "Status", "Sales Rep", "Notes", 
    "Send Mode", "Send Status", "Send Attempts", "Last Error", "Last Sent At",
    "Draft Email", "Email Subject"
]

# Ensure columns exist in DataFrame
for col in all_cols:
    if col not in df.columns:
        df[col] = ""

# Force columns to String to prevent editing errors
df["Salary"] = df["Salary"].astype(str)
df["Contact Info"] = df["Contact Info"].astype(str)
df["Send Status"] = df["Send Status"].astype(str)

# Pre-processing for Sorting
df["_sort_date"] = df["Post Date"].apply(parse_date)

# Define CRM Status Options
STATUS_OPTIONS = ["New", "In Progress", "Hot Lead", "Lost", "Not Relevant"]

# --- SIDEBAR FILTERS ---
with st.sidebar:
    st.header("🔍 Filters & Sorting")
    
    # Sort Options
    sort_cols_selection = st.multiselect(
        "Sort By:", 
        options=["Date", "Status", "Sales Rep", "Salary"],
        default=["Date"]
    )
    sort_ascending = st.checkbox("Ascending Order (A-Z)?", value=False)

    st.divider()
    
    # Filters
    status_filter = st.multiselect("Filter Status:", options=STATUS_OPTIONS, default=[])
    
    existing_reps = df["Sales Rep"].unique().tolist() if "Sales Rep" in df.columns else []
    all_reps = list(set(sales_reps_options + existing_reps))
    rep_filter = st.multiselect("Filter Sales Rep:", options=all_reps, default=[])

# --- FILTERING THE DATA ---
filtered_df = df.copy()

# 1. Filter by Status (Default: Hide 'Lost' unless selected)
if not status_filter:
    filtered_df = filtered_df[~filtered_df["Status"].isin(["Lost", "Not Relevant"])]
else:
    filtered_df = filtered_df[filtered_df["Status"].isin(status_filter)]

# 2. Filter by Rep
if rep_filter:
    filtered_df = filtered_df[filtered_df["Sales Rep"].isin(rep_filter)]

# 3. Apply Sorting
if sort_cols_selection:
    col_map = {"Date": "_sort_date", "Status": "Status", "Sales Rep": "Sales Rep", "Salary": "Salary"}
    actual_sort_cols = [col_map[c] for c in sort_cols_selection]
    filtered_df = filtered_df.sort_values(by=actual_sort_cols, ascending=sort_ascending)
else:
    filtered_df = filtered_df.sort_values(by="_sort_date", ascending=False)

# --- TOP METRICS ---
c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Active Leads", len(filtered_df))
c2.metric("New Leads", len(df[df["Status"] == "New"]))
c3.metric("Hot Leads 🔥", len(df[df["Status"] == "Hot Lead"]))
c4.metric("Sent ✉️", len(df[df["Send Status"] == "SENT"]))
manual_count = len(df[df["Send Status"] == "MANUAL_CHECK"])
c5.metric("Manual Check ✋", manual_count)

st.divider()

# --- MAIN EDITOR TABLE ---
st.info(f"💡 Showing {len(filtered_df)} leads. Edit values below and click 'Save'.")

# Select specific columns to display in the UI
display_columns = [
    "Job Title", "Salary", "Post Date", "Contact Info", "Link", 
    "Description", "Status", "Sales Rep", "Notes", "Draft Email", "Send Status"
]

# Render the Data Editor
edited_view = st.data_editor(
    filtered_df[display_columns],
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
        "Send Status": st.column_config.TextColumn("Send Status", disabled=True), 
    },
    hide_index=True
)

# ---------------------------------------------------------
# 5. SAVE & ARCHIVE LOGIC
# ---------------------------------------------------------
st.divider()

if st.button("💾 Save Changes (Archive 'Lost' Leads)", type="primary"):
    try:
        with st.spinner("Syncing to Google Sheets..."):
            # 1. Update the master DataFrame with user edits
            df.update(edited_view)
            
            # 2. Split Data: Active vs. Archive
            # Rows marked as 'Lost' or 'Not Relevant' will be moved
            rows_to_archive = df[df["Status"].isin(["Lost", "Not Relevant"])].copy()
            rows_to_keep = df[~df["Status"].isin(["Lost", "Not Relevant"])].copy()
            
            # 3. Clean up (Keep only official columns)
            rows_to_archive = rows_to_archive[all_cols]
            rows_to_keep = rows_to_keep[all_cols]
            
            # 4. Handle Archive Sheet ("Lost_Leads")
            if not rows_to_archive.empty:
                try:
                    lost_sheet = main_doc_obj.worksheet("Lost_Leads")
                except gspread.WorksheetNotFound:
                    # Create tab if it doesn't exist
                    lost_sheet = main_doc_obj.add_worksheet(title="Lost_Leads", rows=1000, cols=20)
                    lost_sheet.append_row(all_cols) 
                
                # Append lost leads to the archive tab
                lost_sheet.append_rows(rows_to_archive.values.tolist())
                st.toast(f"📦 Moved {len(rows_to_archive)} leads to 'Lost_Leads' tab.", icon="🗑️")
            
            # 5. Overwrite Main Sheet (Active leads only)
            sheet_obj.clear()
            sheet_obj.update([rows_to_keep.columns.values.tolist()] + rows_to_keep.values.tolist())
        
        st.success("✅ Database saved successfully!")
        time.sleep(1.5)
        st.rerun()
            
    except Exception as e:
        st.error(f"❌ Save Failed: {e}")