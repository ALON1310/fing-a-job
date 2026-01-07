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
    try:
        creds = ServiceAccountCredentials.from_json_keyfile_name("credentials.json", scope)
        return gspread.authorize(creds)
    except Exception:
        pass
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
        st.error("🚨 Authentication Error.")
        st.stop()
    try:
        sheet = client.open("Master_Leads_DB").sheet1
        data = sheet.get_all_records()
        df = pd.DataFrame(data)
        try:
            settings_sheet = client.open("Master_Leads_DB").worksheet("Settings")
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
    if not isinstance(date_str, str): return pd.NaT
    try:
        clean = date_str.replace(",", "").strip()
        return pd.to_datetime(clean, format="%b %d %Y", errors='coerce')
    except:
        return pd.NaT

# --- MAIN APP ---
df, sheet_obj, sales_reps_options = load_data()

expected_cols = ["Job Title", "Salary", "Post Date", "Contact Info", "Link", "Description", "Status", "Sales Rep", "Notes"]
for col in expected_cols:
    if col not in df.columns: df[col] = ""

# --- DEFINED STATUS LIST (The Professional Way) ---
STATUS_OPTIONS = ["New", "In Progress", "Hot Lead", "Lost", "Not Relevant"]

# --- PRE-PROCESSING ---
df["_sort_date"] = df["Post Date"].apply(parse_date)

# --- SIDEBAR ---
with st.sidebar:
    st.header("🔍 Filters & Sorting")
    
    st.subheader("Sort Options")
    sort_cols_selection = st.multiselect(
        "Sort By:", 
        options=["Date", "Status", "Sales Rep", "Salary"],
        default=["Date"]
    )
    sort_ascending = st.checkbox("Ascending Order (A-Z)?", value=False)
    
    st.divider()
    
    # STATUS FILTER
    status_filter = st.multiselect(
        "Filter Status:", 
        options=STATUS_OPTIONS,
        default=[] 
    )
    
    # REP FILTER
    existing_reps_in_db = df["Sales Rep"].unique().tolist() if "Sales Rep" in df.columns else []
    all_reps_combined = list(set(sales_reps_options + existing_reps_in_db))
    rep_filter = st.multiselect("Filter Sales Rep:", options=all_reps_combined, default=[])

# --- APPLY LOGIC ---

# 1. Status Logic: Hide "Lost" and "Not Relevant" unless explicitly selected
if not status_filter:
    # Show everything EXCEPT dead leads
    filtered_df = df[~df["Status"].isin(["Lost", "Not Relevant"])]
else:
    # Show ONLY what was selected
    filtered_df = df[df["Status"].isin(status_filter)]

# 2. Rep Logic
if rep_filter:
    filtered_df = filtered_df[filtered_df["Sales Rep"].isin(rep_filter)]

# 3. Sorting Logic
if sort_cols_selection:
    col_map = {"Date": "_sort_date", "Status": "Status", "Sales Rep": "Sales Rep", "Salary": "Salary"}
    actual_sort_cols = [col_map[c] for c in sort_cols_selection]
    filtered_df = filtered_df.sort_values(by=actual_sort_cols, ascending=sort_ascending)

display_df = filtered_df.drop(columns=["_sort_date"])

# --- METRICS ---
c1, c2, c3, c4 = st.columns(4)
c1.metric("Total Active Leads", len(display_df))
c2.metric("New Leads", len(df[df["Status"] == "New"]))
c3.metric("Hot Leads 🔥", len(df[df["Status"] == "Hot Lead"]))
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

# --- SAVE ---
if st.button("💾 Save to Google Sheets", type="primary"):
    try:
        if len(display_df) < len(df):
             st.warning("⚠️ Saving filtered view. For full data safety, clear filters first.")
        
        sheet_obj.clear()
        sheet_obj.append_row(edited_df.columns.tolist())
        sheet_obj.append_rows(edited_df.values.tolist())
        st.toast("✅ Updated!", icon="☁️")
        time.sleep(1)
        st.rerun()
    except Exception as e:
        st.error(f"Save Failed: {e}")