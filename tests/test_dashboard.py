import streamlit as st
import pandas as pd
import gspread
import time
import os
from dotenv import load_dotenv

# --- IMPORTS FROM OUR CENTRAL CLIENT ---
# This is the line that was missing or different
from sheets_client import get_sheet_client

# ---------------------------------------------------------
# 1. CONFIGURATION & SETUP
# ---------------------------------------------------------

st.set_page_config(page_title="Platonics CRM", page_icon="☁️", layout="wide")
st.title("☁️ Platonics Lead Manager")

load_dotenv()

# ---------------------------------------------------------
# 2. DATA LOADING
# ---------------------------------------------------------

def load_data():
    """
    Connects to the Sheet using the centralized sheets_client logic.
    """
    try:
        # Use the imported function instead of defining a local one
        client = get_sheet_client()
    except Exception as e:
        st.error(f"🚨 Authentication Error: {e}")
        st.stop()
        
    sheet_name = os.getenv("SHEET", "Master_Leads_DB")
    
    try:
        main_doc = client.open(sheet_name)
        sheet = main_doc.sheet1
        data = sheet.get_all_records()
        df = pd.DataFrame(data)
        
        # Safe Date Parsing for Sorting
        if "Post Date" in df.columns:
             df["_sort_date"] = pd.to_datetime(df["Post Date"], format="%b %d %Y", errors='coerce')
        else:
             df["_sort_date"] = pd.NaT

        sales_reps_options = ["Dor", "Alon", "Unassigned"]
            
        return df, sheet, main_doc, sales_reps_options
    except Exception as e:
        st.error(f"⚠️ Error connecting to Google Sheet '{sheet_name}': {e}")
        st.stop()

# Helper: Parse Dates safely (Used internally for display if needed)
def parse_date(date_str):
    if not isinstance(date_str, str):
        return pd.NaT
    try:
        clean = date_str.replace(",", "").strip()
        return pd.to_datetime(clean, format="%b %d %Y", errors='coerce')
    except Exception:
        return pd.NaT

# ---------------------------------------------------------
# 3. APP LOGIC
# ---------------------------------------------------------

# Load Data
df, sheet_obj, main_doc_obj, sales_reps_options = load_data()

# Define ALL columns
all_cols = [
    "Job Title", "Salary", "Post Date", "Contact Info", "Link", "Description", 
    "Status", "Sales Rep", "Notes", 
    "Send Mode", "Send Status", "Send Attempts", "Last Error", "Last Sent At",
    "Followup Count", "Draft Email", "Email Subject"
]

# Ensure columns exist
for col in all_cols:
    if col not in df.columns:
        df[col] = ""

# Force string types
df["Salary"] = df["Salary"].astype(str)
df["Contact Info"] = df["Contact Info"].astype(str)
df["Send Status"] = df["Send Status"].astype(str)
df["Followup Count"] = df["Followup Count"].astype(str) 

STATUS_OPTIONS = ["New", "Follow-up", "In Progress", "Hot Lead", "Lost", "Not Relevant"]

# --- SIDEBAR FILTERS ---
with st.sidebar:
    st.header("🔍 Filters & Sorting")
    
    sort_cols_selection = st.multiselect(
        "Sort By:", 
        options=["Date", "Status", "Sales Rep"],
        default=["Date"]
    )
    sort_ascending = st.checkbox("Ascending Order (A-Z)?", value=False)

    st.divider()
    
    status_filter = st.multiselect("Filter Status:", options=STATUS_OPTIONS, default=[])
    
    existing_reps = df["Sales Rep"].unique().tolist() if "Sales Rep" in df.columns else []
    all_reps = list(set(sales_reps_options + existing_reps))
    rep_filter = st.multiselect("Filter Sales Rep:", options=all_reps, default=[])

# --- FILTERING ---
filtered_df = df.copy()

if not status_filter:
    filtered_df = filtered_df[~filtered_df["Status"].isin(["Lost", "Not Relevant"])]
else:
    filtered_df = filtered_df[filtered_df["Status"].isin(status_filter)]

if rep_filter:
    filtered_df = filtered_df[filtered_df["Sales Rep"].isin(rep_filter)]

# Sorting logic
if sort_cols_selection:
    col_map = {"Date": "_sort_date", "Status": "Status", "Sales Rep": "Sales Rep"}
    actual_sort_cols = [col_map[c] for c in sort_cols_selection if c in col_map]
    
    if actual_sort_cols:
        filtered_df = filtered_df.sort_values(by=actual_sort_cols, ascending=sort_ascending)
else:
    filtered_df = filtered_df.sort_values(by="_sort_date", ascending=False)

# --- METRICS ---
c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Active Leads", len(filtered_df))
c2.metric("New Leads", len(df[df["Status"] == "New"]))
c3.metric("Follow-ups ⏳", len(df[df["Status"] == "Follow-up"]))
c4.metric("Sent ✉️", len(df[df["Send Status"] == "SENT"]))
manual_count = len(df[df["Send Status"] == "MANUAL_CHECK"])
c5.metric("Manual Check ✋", manual_count)

st.divider()

# --- EDITOR ---
st.info(f"💡 Showing {len(filtered_df)} leads. Edit values below and click 'Save'.")

display_columns = [
    "Job Title", "Salary", "Post Date", "Contact Info", "Link", 
    "Description", "Status", "Followup Count", "Sales Rep", "Notes", "Draft Email", "Send Status"
]

valid_display_cols = [c for c in display_columns if c in filtered_df.columns]

edited_view = st.data_editor(
    filtered_df[valid_display_cols],
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
        "Followup Count": st.column_config.TextColumn("Retries", width="small", disabled=True),
    },
    hide_index=True
)

# --- SAVE LOGIC ---
st.divider()

if st.button("💾 Save Changes (Archive 'Lost' Leads)", type="primary"):
    try:
        with st.spinner("Syncing to Google Sheets..."):
            # Update Master DF
            df.update(edited_view)
            
            # Split Data
            rows_to_archive = df[df["Status"].isin(["Lost", "Not Relevant"])].copy()
            rows_to_keep = df[~df["Status"].isin(["Lost", "Not Relevant"])].copy()
            
            # Keep clean columns
            rows_to_archive = rows_to_archive[all_cols]
            rows_to_keep = rows_to_keep[all_cols]
            
            # Archive
            if not rows_to_archive.empty:
                try:
                    lost_sheet = main_doc_obj.worksheet("Lost_Leads")
                except gspread.WorksheetNotFound:
                    lost_sheet = main_doc_obj.add_worksheet(title="Lost_Leads", rows=1000, cols=20)
                    lost_sheet.append_row(all_cols) 
                
                lost_sheet.append_rows(rows_to_archive.values.tolist())
                st.toast(f"📦 Moved {len(rows_to_archive)} leads to 'Lost_Leads' tab.", icon="🗑️")
            
            # Update Main Sheet
            sheet_obj.clear()
            sheet_obj.update([rows_to_keep.columns.values.tolist()] + rows_to_keep.values.tolist())
        
        st.success("✅ Database saved successfully!")
        time.sleep(1.5)
        st.rerun()
            
    except Exception as e:
        st.error(f"❌ Save Failed: {e}")