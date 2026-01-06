import streamlit as st
import pandas as pd
import os
import glob
import time

# --- UI SETTINGS ---
st.set_page_config(page_title="Job Leads CRM", page_icon="💼", layout="wide")

st.title("💼 Sales Lead CRM")

# --- FUNCTION TO GET LATEST FILE ---
def get_latest_excel():
    list_of_files = glob.glob('*.xlsx') # Find all Excel files
    if not list_of_files:
        return None
    latest_file = max(list_of_files, key=os.path.getctime)
    return latest_file

excel_file = get_latest_excel()

if excel_file:
    st.success(f"📂 Active File: **{excel_file}**")
    
    # LOAD DATA
    try:
        df = pd.read_excel(excel_file, sheet_name='Sales Pipeline')
    except Exception as e:
        st.error(f"Could not read file. Ensure it is closed! Error: {e}")
        st.stop()

    # Ensure critical columns exist (prevents crash on old files)
    required_cols = ["Status", "Sales Rep", "Link", "Contact Info", "Description"]
    for col in required_cols:
        if col not in df.columns:
            df[col] = "" 

    # --- SIDEBAR FILTERS ---
    with st.sidebar:
        st.header("🔍 Search & Filters")
        
        # Status Filter
        all_statuses = df["Status"].unique().tolist()
        status_filter = st.multiselect(
            "Filter by Status:",
            options=all_statuses,
            default=all_statuses
        )
        
        # Sales Rep Filter
        all_reps = df["Sales Rep"].unique().tolist()
        rep_filter = st.multiselect(
            "Filter by Sales Rep:",
            options=all_reps,
            default=all_reps
        )
    
    # APPLY FILTERS
    filtered_df = df[
        (df["Status"].isin(status_filter)) & 
        (df["Sales Rep"].isin(rep_filter))
    ]

    # --- INTERACTIVE TABLE ---
    st.markdown("### 👇 Edit Data Here (Changes save automatically on button click)")
    
    edited_df = st.data_editor(
        filtered_df,
        num_rows="dynamic",
        use_container_width=True,
        column_config={
            "Link": st.column_config.LinkColumn(
                "Link",
                display_text="Open Link"
            ),
            "Status": st.column_config.SelectboxColumn(
                "Status",
                width="medium",
                options=[
                    "New",
                    "In Progress",
                    "Hot Lead", 
                    "Closed",
                    "Not Relevant"
                ],
                required=True,
            ),
            "Sales Rep": st.column_config.SelectboxColumn(
                "Sales Rep",
                width="medium",
                options=[
                    "Dor",
                    "Alon",
                    "Unassigned"
                ],
            ),
            "Contact Info": st.column_config.TextColumn(
                "Contact Info",
                width="large"
            ),
             "Description": st.column_config.TextColumn(
                "Description",
                width="large"
            ),
        },
        hide_index=True,
    )

    # --- SAVE BUTTON ---
    if st.button("💾 Save Changes to Excel", type="primary"):
        try:
            # Save logic
            with pd.ExcelWriter(excel_file, engine='xlsxwriter') as writer:
                edited_df.to_excel(writer, sheet_name='Sales Pipeline', index=False)
                
            st.toast("✅ File saved successfully!", icon="🎉")
            time.sleep(1) 
            st.rerun()    # Refresh to show updated data
            
        except PermissionError:
            st.error("⚠️ Error: Excel file is OPEN. Please close it and try again!")
        except Exception as e:
            st.error(f"Error saving file: {e}")

    # --- QUICK METRICS ---
    st.divider()
    c1, c2, c3 = st.columns(3)
    c1.metric("Total Leads Shown", len(edited_df))
    
    # Safe check for metrics
    if "Status" in edited_df.columns:
        hot_leads = len(edited_df[edited_df['Status'] == 'Hot Lead'])
        new_leads = len(edited_df[edited_df['Status'] == 'New'])
        c2.metric("Hot Leads", hot_leads)
        c3.metric("New Leads", new_leads)

else:
    st.warning("⚠️ No Excel files found. Please run the Scraper (test_scraper.py) first!")