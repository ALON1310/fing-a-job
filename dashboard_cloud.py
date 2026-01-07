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
    
    # Attempt 1: Local loading (for testing on Mac)
    try:
        creds = ServiceAccountCredentials.from_json_keyfile_name("credentials.json", scope)
        return gspread.authorize(creds)
    except Exception:
        pass
    
    # Attempt 2: Cloud loading (Streamlit Secrets)
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
        st.error("🚨 Authentication Error. Please check secrets or credentials.json")
        st.stop()
        
    try:
        # 1. Load Main Data
        sheet = client.open("Master_Leads_DB").sheet1
        data = sheet.get_all_records()
        df = pd.DataFrame(data)

        # 2. Load Settings (Sales Reps List)
        try:
            settings_sheet = client.open("Master_Leads_DB").worksheet("Settings")
            reps_list = settings_sheet.col_values(1)
            # Filter out header and empty rows
            sales_reps_options = [x for x in reps_list if x and x != "Sales Reps"]
        except gspread.WorksheetNotFound:
            # Fallback if Settings tab doesn't exist
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

# Ensure critical columns exist (Sync with Scraper)
expected_cols = ["Job Title", "Salary", "Post Date", "Contact Info", "Link", "Description", "Status", "Sales Rep", "Notes"]
for col in expected_cols:
    if col not in df.columns:
        df[col] = ""

# --- DEFINED STATUS LIST (Professional Logic) ---
STATUS_OPTIONS = ["New", "In Progress", "Hot Lead", "Lost", "Not Relevant"]

# --- DATA PRE-PROCESSING ---
# Create temporary sorting column
df["_sort_date"] = df["Post Date"].apply(parse_date)

# --- SIDEBAR CONTROLS ---
with st.sidebar:
    st.header("🔍 Filters & Sorting")
    
    # --- 1. MULTI-SORTING ---
    st.subheader("Sort Options")
    sort_cols_selection = st.multiselect(
        "Sort By (Order Matters):", 
        options=["Date", "Status", "Sales Rep", "Salary"],
        default=["Date"]
    )
    sort_ascending = st.checkbox("Ascending Order (A-Z / Oldest First)?", value=False)
    
    st.divider()
    
    # --- 2. STATUS FILTER ---
    # Logic: Default view hides "Lost" & "Not Relevant"
    status_filter = st.multiselect(
        "Filter Status:", 
        options=STATUS_OPTIONS,
        default=[] # Start empty implies "Default View"
    )
    
    # --- 3. SALES REP FILTER ---
    existing_reps_in_db = df["Sales Rep"].unique().tolist() if "Sales Rep" in df.columns else []
    all_reps_combined = list(set(sales_reps_options + existing_reps_in_db))
    
    rep_filter = st.multiselect(
        "Filter Sales Rep:", 
        options=all_reps_combined,
        default=[] 
    )

# --- APPLY LOGIC ---

# 1. Apply Status Filter (Smart Logic)
if not status_filter:
    # If no filter selected, show everything EXCEPT hidden statuses
    # This automatically hides "Lost" rows unless specifically asked for
    filtered_df = df[~df["Status"].isin(["Lost", "Not Relevant"])]
else:
    # If user selected specific statuses (e.g., explicitly selected "Lost"), show only them
    filtered_df = df[df["Status"].isin(status_filter)]

# 2. Apply Rep Filter
if rep_filter:
    filtered_df = filtered_df[filtered_df["Sales Rep"].isin(rep_filter)]

# 3. Apply Multi-Sorting
if sort_cols_selection:
    # Map friendly names to actual dataframe column names
    col_map = {
        "Date": "_sort_date",
        "Status": "Status",
        "Sales Rep": "Sales Rep",
        "Salary": "Salary"
    }
    # Create list of actual columns to sort by
    actual_sort_cols = [col_map[c] for c in sort_cols_selection]
    
    # Perform the sort
    filtered_df = filtered_df.sort_values(by=actual_sort_cols, ascending=sort_ascending)

# Remove helper column before display
display_df = filtered_df.drop(columns=["_sort_date"])

# --- METRICS ---
c1, c2, c3, c4 = st.columns(4)
c1.metric("Total Active Leads", len(display_df))
c2.metric("New Leads", len(df[df["Status"] == "New"]))
c3.metric("Hot Leads 🔥", len(df[df["Status"] == "Hot Lead"]))
# Show Lost count from total DB, not just filtered view
c4.metric("Conversion Fail (Lost)", len(df[df["Status"] == "Lost"]))

st.divider()

# --- EDITOR ---
st.info(f"💡 Showing {len(display_df)} active leads.")

edited_df = st.data_editor(
    display_df,
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
        "Post Date": st.column_config.TextColumn("Post Date", width="small"),
    },
    hide_index=True
)

# --- SAVE BUTTON ---
if st.button("💾 Save to Google Sheets", type="primary"):
    try:
        # Warning if saving filtered view
        if len(display_df) < len(df):
             st.warning("⚠️ Warning: You are viewing a filtered list. Saving now updates only the visible rows, but syncs fully to Sheets.")
        
        sheet_obj.clear()
        sheet_obj.append_row(edited_df.columns.tolist())
        sheet_obj.append_rows(edited_df.values.tolist())
        
        st.toast("✅ Google Sheet Updated!", icon="☁️")
        time.sleep(1)
        st.rerun()
            
    except Exception as e:
        st.error(f"Save Failed: {e}")