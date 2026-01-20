import streamlit as st
import pandas as pd
import gspread
import time
import os
from dotenv import load_dotenv

# --- IMPORTS FROM CENTRAL CLIENT ---
from sheets_client import get_sheet_client

# --- 🕵️‍♂️ DEBUG START ---
st.title("🔍 Secrets Debugger")

# 1. בדיקה האם Streamlit מזהה סודות בכלל
try:
    secrets_keys = list(st.secrets.keys())
    st.write(f"📂 Available Secret Sections: `{secrets_keys}`")
    
    # 2. בדיקה ספציפית לסקשן שלנו
    if "GCP_SERVICE_ACCOUNT" in st.secrets:
        st.success("✅ Found [GCP_SERVICE_ACCOUNT] section!")
        inner_keys = list(st.secrets["GCP_SERVICE_ACCOUNT"].keys())
        st.write(f"🔑 Keys inside: `{inner_keys}`")
        
        # 3. בדיקה קריטית - האם ה-Private Key נראה תקין?
        pk = st.secrets["GCP_SERVICE_ACCOUNT"].get("private_key", "")
        if "-----BEGIN PRIVATE KEY-----" in pk:
            st.success("✅ Private Key structure looks valid.")
        else:
            st.error("❌ Private Key is missing or malformed!")
    else:
        st.error("❌ [GCP_SERVICE_ACCOUNT] section is MISSING in secrets!")

except FileNotFoundError:
    st.error("❌ No secrets file found at all.")
except Exception as e:
    st.error(f"❌ Error reading secrets: {e}")

st.divider()
# --- 🕵️‍♂️ DEBUG END ---
# ---------------------------------------------------------
# 1. CONFIGURATION & SETUP
# ---------------------------------------------------------
st.set_page_config(page_title="Platonics CRM", page_icon="☁️", layout="wide")
st.title("☁️ Platonics Lead Manager")

load_dotenv()

# ---------------------------------------------------------
# 2. DATA LOADING (DYNAMIC REPS)
# ---------------------------------------------------------

def load_data():
    """
    1. Connects to Google Sheets using the centralized client.
    2. Loads Leads from 'Master_Leads_DB' (or env var).
    3. Loads Sales Reps dynamically from 'Settings' tab.
    """
    try:
        # Use the centralized client function
        client = get_sheet_client()
    except Exception as e:
        st.error(f"🚨 Authentication Error: {e}")
        st.stop()
        
    sheet_name = os.getenv("SHEET", "Master_Leads_DB")
    
    try:
        main_doc = client.open(sheet_name)
        
        # --- A. Load Main Leads Data ---
        sheet = main_doc.sheet1
        data = sheet.get_all_records()
        df = pd.DataFrame(data)
        
        # Safe Date Parsing for Sorting purposes
        if "Post Date" in df.columns:
             df["_sort_date"] = pd.to_datetime(df["Post Date"], format="%b %d %Y", errors='coerce')
        else:
             df["_sort_date"] = pd.NaT

        # --- B. Load Sales Reps from Settings Tab ---
        sales_reps_options = ["Dor", "Alon", "Unassigned"]
        
        try:
            try:
                settings_sheet = main_doc.worksheet("Settings")
            except gspread.WorksheetNotFound:
                settings_sheet = main_doc.worksheet("Setting")
            
            settings_data = settings_sheet.get_all_records()
            settings_df = pd.DataFrame(settings_data)
            
            rep_col = None
            for col in settings_df.columns:
                if "Sales Rep" in col: 
                    rep_col = col
                    break
            
            if rep_col:
                dynamic_reps = settings_df[rep_col].astype(str).str.strip().unique().tolist()
                dynamic_reps = [r for r in dynamic_reps if r and r.lower() != "none"]
                
                if dynamic_reps:
                    sales_reps_options = sorted(dynamic_reps)
                    if "Unassigned" not in sales_reps_options:
                        sales_reps_options.append("Unassigned")
            
        except Exception:
            pass
            
        return df, sheet, main_doc, sales_reps_options

    except Exception as e:
        st.error(f"⚠️ Error connecting to Google Sheet '{sheet_name}': {e}")
        st.stop()

# ---------------------------------------------------------
# 3. APP LOGIC
# ---------------------------------------------------------

# Load Data
df, sheet_obj, main_doc_obj, sales_reps_options = load_data()

# Define ALL standard columns
all_cols = [
    "Job Title", "Salary", "Post Date", "Contact Info", "Link", "Description", 
    "Status", "Sales Rep", "Notes", 
    "Send Mode", "Send Status", "Send Attempts", "Last Error", "Last Sent At",
    "Followup Count", "Draft Email", "Email Subject"
]

# Ensure all columns exist in DataFrame
for col in all_cols:
    if col not in df.columns:
        df[col] = ""

# Force specific types to string
df["Salary"] = df["Salary"].astype(str)
df["Contact Info"] = df["Contact Info"].astype(str)
df["Send Status"] = df["Send Status"].astype(str)
df["Followup Count"] = df["Followup Count"].astype(str) 

STATUS_OPTIONS = ["New", "Follow-up", "In Progress", "Hot Lead", "Lost", "Not Relevant"]

# --- SIDEBAR FILTERS ---
with st.sidebar:
    st.header("🔍 Filters & Sorting")
    
    # Sort Options
    sort_cols_selection = st.multiselect(
        "Sort By:", 
        options=["Date", "Status", "Sales Rep", "Send Status"],
        default=["Date"]
    )
    sort_ascending = st.checkbox("Ascending Order (A-Z)?", value=False)

    st.divider()
    
    # CRM Status Filter
    status_filter = st.multiselect("Filter CRM Status:", options=STATUS_OPTIONS, default=[])
    
    # Rep Filter
    existing_reps_in_data = df["Sales Rep"].unique().tolist() if "Sales Rep" in df.columns else []
    all_reps_combined = list(set(sales_reps_options + existing_reps_in_data))
    rep_filter = st.multiselect("Filter Sales Rep:", options=all_reps_combined, default=[])

    st.divider()
    
    # --- NEW: EMAIL AUTOMATION FILTERS ---
    st.subheader("📧 Email Automation")
    
    # Get unique Send Statuses (e.g., SENT, PENDING, MANUAL_CHECK, FAILED)
    unique_send_statuses = sorted([s for s in df["Send Status"].unique() if s])
    send_status_filter = st.multiselect("Filter Send Status:", options=unique_send_statuses, default=[])

# --- FILTERING ---
filtered_df = df.copy()

# 1. Status Filter (Default: Hide 'Lost' unless explicitly selected)
if not status_filter:
    filtered_df = filtered_df[~filtered_df["Status"].isin(["Lost", "Not Relevant"])]
else:
    filtered_df = filtered_df[filtered_df["Status"].isin(status_filter)]

# 2. Rep Filter
if rep_filter:
    filtered_df = filtered_df[filtered_df["Sales Rep"].isin(rep_filter)]

# 3. NEW: Send Status Filter (For Manual Check)
if send_status_filter:
    filtered_df = filtered_df[filtered_df["Send Status"].isin(send_status_filter)]

# 4. Sorting Logic
if sort_cols_selection:
    col_map = {
        "Date": "_sort_date", 
        "Status": "Status", 
        "Sales Rep": "Sales Rep",
        "Send Status": "Send Status"
    }
    actual_sort_cols = [col_map[c] for c in sort_cols_selection if c in col_map]
    
    if actual_sort_cols:
        filtered_df = filtered_df.sort_values(by=actual_sort_cols, ascending=sort_ascending)
else:
    filtered_df = filtered_df.sort_values(by="_sort_date", ascending=False)

# --- TOP METRICS ---
c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Active Leads", len(filtered_df))
c2.metric("New Leads", len(df[df["Status"] == "New"]))
c3.metric("Follow-ups ⏳", len(df[df["Status"] == "Follow-up"]))
c4.metric("Sent ✉️", len(df[df["Send Status"] == "SENT"]))
manual_count = len(df[df["Send Status"] == "MANUAL_CHECK"])
c5.metric("Manual Check ✋", manual_count)

st.divider()

# --- MAIN EDITOR TABLE ---
st.info(f"💡 Showing {len(filtered_df)} leads. Edit values below and click 'Save'.")

# Select specific columns for UI display
display_columns = [
    "Job Title", "Salary", "Post Date", "Contact Info", "Link", 
    "Description", "Status", "Followup Count", "Sales Rep", "Notes", "Draft Email", "Send Status", "Last Error"
]

valid_display_cols = [c for c in display_columns if c in filtered_df.columns]

# Render Data Editor
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
        "Last Error": st.column_config.TextColumn("Error Log", width="medium", disabled=True),
    },
    hide_index=True
)

# --- SAVE & ARCHIVE LOGIC ---
st.divider()

if st.button("💾 Save Changes (Archive 'Lost' Leads)", type="primary"):
    try:
        with st.spinner("Syncing to Google Sheets..."):
            # 1. Update master DataFrame with edits
            df.update(edited_view)
            
            # 2. Split Data: Active vs. Archive
            rows_to_archive = df[df["Status"].isin(["Lost", "Not Relevant"])].copy()
            rows_to_keep = df[~df["Status"].isin(["Lost", "Not Relevant"])].copy()
            
            # 3. Clean columns
            rows_to_archive = rows_to_archive[all_cols]
            rows_to_keep = rows_to_keep[all_cols]
            
            # 4. Handle Archive ('Lost_Leads' tab)
            if not rows_to_archive.empty:
                try:
                    lost_sheet = main_doc_obj.worksheet("Lost_Leads")
                except gspread.WorksheetNotFound:
                    # Create tab if missing
                    lost_sheet = main_doc_obj.add_worksheet(title="Lost_Leads", rows=1000, cols=20)
                    lost_sheet.append_row(all_cols) 
                
                # Append lost leads
                lost_sheet.append_rows(rows_to_archive.values.tolist())
                st.toast(f"📦 Moved {len(rows_to_archive)} leads to 'Lost_Leads' tab.", icon="🗑️")
            
            # 5. Update Main Sheet
            sheet_obj.clear()
            sheet_obj.update([rows_to_keep.columns.values.tolist()] + rows_to_keep.values.tolist())
        
        st.success("✅ Database saved successfully!")
        time.sleep(1.5)
        st.rerun()
            
    except Exception as e:
        st.error(f"❌ Save Failed: {e}")