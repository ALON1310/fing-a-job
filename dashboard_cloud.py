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
    
    # Attempt 1: Local loading
    try:
        creds = ServiceAccountCredentials.from_json_keyfile_name("credentials.json", scope)
        return gspread.authorize(creds)
    except Exception:
        pass
    
    # Attempt 2: Cloud loading
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

# --- LOAD DATA & SETTINGS ---
def load_data():
    client = get_gspread_client()
    if not client:
        st.error("🚨 Authentication Error.")
        st.stop()
        
    try:
        # 1. Load Main Data
        sheet = client.open("Master_Leads_DB").sheet1
        data = sheet.get_all_records()
        df = pd.DataFrame(data)

        # 2. Load Settings (Sales Reps List)
        try:
            settings_sheet = client.open("Master_Leads_DB").worksheet("Settings")
            # Get all values from Column A (Sales Reps)
            reps_list = settings_sheet.col_values(1)
            # Filter out the header "Sales Reps" and empty strings
            sales_reps_options = [x for x in reps_list if x and x != "Sales Reps"]
        except gspread.WorksheetNotFound:
            # Fallback if user forgot to create Settings tab
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

# Ensure critical columns exist
expected_cols = ["Job Title", "Salary", "Post Date", "Contact Info", "Link", "Description", "Status", "Sales Rep", "Notes"]
for col in expected_cols:
    if col not in df.columns:
        df[col] = ""

# --- DATA PRE-PROCESSING ---
df["_sort_date"] = df["Post Date"].apply(parse_date)

# --- SIDEBAR ---
with st.sidebar:
    st.header("🔍 Filters & Sorting")
    
    sort_by = st.radio("Sort By:", ["Date (Newest First)", "Status", "Sales Rep"])
    
    st.divider()
    
    # Use dynamic options here too!
    status_options = df["Status"].unique().tolist() if "Status" in df.columns else []
    
    # Combined list for filter (what's in DB + what's in Settings)
    existing_reps_in_db = df["Sales Rep"].unique().tolist() if "Sales Rep" in df.columns else []
    all_reps_combined = list(set(sales_reps_options + existing_reps_in_db))
    
    status_filter = st.multiselect("Filter Status:", options=status_options, default=status_options)
    rep_filter = st.multiselect("Filter Sales Rep:", options=all_reps_combined, default=all_reps_combined)

# --- LOGIC ---
if sort_by == "Date (Newest First)":
    df = df.sort_values(by="_sort_date", ascending=False)
elif sort_by == "Status":
    df = df.sort_values(by="Status")
elif sort_by == "Sales Rep":
    df = df.sort_values(by="Sales Rep")

filtered_df = df[
    (df["Status"].isin(status_filter)) & 
    (df["Sales Rep"].isin(rep_filter))
]

display_df = filtered_df.drop(columns=["_sort_date"])

# --- METRICS ---
c1, c2, c3 = st.columns(3)
c1.metric("Total Leads", len(df))
c2.metric("New Leads", len(df[df["Status"] == "New"]))
c3.metric("Hot Leads 🔥", len(df[df["Status"] == "Hot Lead"]))

st.divider()

# --- EDITOR ---
st.info(f"💡 Showing {len(display_df)} leads. Sales Reps list loaded from 'Settings' tab.")

edited_df = st.data_editor(
    display_df,
    num_rows="dynamic",
    use_container_width=True,
    height=600,
    column_config={
        "Link": st.column_config.LinkColumn("Link", display_text="Open"),
        "Status": st.column_config.SelectboxColumn("Status", options=["New", "In Progress", "Hot Lead", "Closed", "Not Relevant"], required=True),
        # HERE IS THE MAGIC - DYNAMIC LIST:
        "Sales Rep": st.column_config.SelectboxColumn("Sales Rep", options=sales_reps_options),
        "Description": st.column_config.TextColumn("Description", width="large"),
        "Contact Info": st.column_config.TextColumn("Contact Info", width="medium"),
        "Post Date": st.column_config.TextColumn("Post Date", width="small"),
    },
    hide_index=True
)

# --- SAVE BUTTON ---
if st.button("💾 Save to Google Sheets", type="primary"):
    try:
        if len(filtered_df) < len(df):
            st.warning("⚠️ Warning: You are viewing a filtered list. Clear filters before saving!")
        else:
            sheet_obj.clear()
            sheet_obj.append_row(edited_df.columns.tolist())
            sheet_obj.append_rows(edited_df.values.tolist())
            
            st.toast("✅ Google Sheet Updated!", icon="☁️")
            time.sleep(1)
            st.rerun()
            
    except Exception as e:
        st.error(f"Save Failed: {e}")