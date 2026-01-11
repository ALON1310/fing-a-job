#!/usr/bin/env python3
"""
automation_manager.py (FINAL & SAFE)
Daily Logic:
1. Archive leads with 5+ follow-ups to 'Lost_Leads'.
2. Send scheduled follow-ups (every 7 days) to 'Follow-up' status leads.
"""

import os
import time
import logging
import gspread
import pandas as pd
from dotenv import load_dotenv

# --- IMPORTS ---
from sheets_client import get_sheet_client
from utils import setup_logging, get_timestamp_iso, extract_email, get_days_diff
from sender_agent import send_real

# --- CONFIG ---
load_dotenv()
setup_logging()

SHEET_NAME = os.getenv("SHEET", "Master_Leads_DB")
MAX_FOLLOWUPS = 5
DAYS_BETWEEN_SENDS = 7
MODE = os.getenv("MODE", "DRYRUN").upper()

# --- TEMPLATES ---
TEMPLATES = {
    2: {
        "body": "Hi {name},\n\nJust floating this to the top of your inbox in case you missed my previous email.\n\nWould you be open to a quick chat about your hiring needs?\n\nBest,\nAlon"
    },
    3: {
        "body": "Hi {name},\n\nI know things get busy, so I'll keep this brief.\n\nI'm still very interested in the position. Are you still looking for help with this?\n\nBest,\nAlon"
    },
    4: {
        "body": "Hi {name},\n\nChecking in one last time regarding the role.\n\nIf you've already filled it, no worries at all—just let me know so I can stop bothering you!\n\nThanks,\nAlon"
    },
    5: {
        "body": "Hi {name},\n\nSince I haven't heard back, I'll assume this isn't the right time.\n\nI'll stop following up now, but feel free to reach out in the future if you need a strong developer.\n\nBest of luck,\nAlon"
    }
}

def process_daily_automation():
    logging.info(f"🤖 Automation Manager Starting | MODE={MODE}")
    
    client = get_sheet_client()
    try:
        main_doc = client.open(SHEET_NAME)
        ws = main_doc.sheet1
        
        # Load data
        data = ws.get_all_records()
        if not data:
            logging.warning("No data found.")
            return

        df = pd.DataFrame(data)
        
        # Ensure columns exist in DataFrame for logic calculation
        for col in ["Followup Count", "Last Sent At", "Status", "Contact Info", "Email Subject", "Job Title"]:
            if col not in df.columns:
                df[col] = ""

        # --- CRITICAL FIX: Safe Column Mapping ---
        # Read the actual header row from Sheets to ensure we write to the correct cells
        header_row = ws.row_values(1)
        try:
            # Finding 1-based index for Gspread directly from the sheet headers
            idx_sent_at = header_row.index("Last Sent At") + 1
            idx_count = header_row.index("Followup Count") + 1
            idx_status = header_row.index("Send Status") + 1
        except ValueError as e:
            logging.error(f"❌ Critical Error: Required column missing in sheet! {e}")
            return

    except Exception as e:
        logging.error(f"Failed to load sheet: {e}")
        return

    rows_to_archive = []
    rows_to_delete_indices = []

    # Iterate rows (index + 2 for Sheet Row Number)
    for i, row in df.iterrows():
        row_num = i + 2 
        
        status = str(row["Status"]).strip()
        contact = str(row["Contact Info"]).strip()
        last_sent = str(row["Last Sent At"]).strip()
        
        try:
            # Handle empty or non-numeric count safely
            raw_count = row["Followup Count"]
            if raw_count == "" or raw_count is None:
                count = 0
            else:
                count = int(raw_count)
        except Exception:
            count = 0
            
        email = extract_email(contact)

        # -------------------------------------------------------
        # LOGIC 1: ARCHIVE LOST LEADS
        # -------------------------------------------------------
        if status == "Follow-up" and count >= MAX_FOLLOWUPS:
            days_since = get_days_diff(last_sent)
            
            if days_since >= 3: 
                logging.info(f"🗑️ Archiving Row {row_num} (Max followups reached)")
                
                archive_row = row.copy()
                archive_row["Status"] = "Lost"
                archive_row["Notes"] = f"{row.get('Notes', '')} | Auto-archived after 5 attempts"
                
                rows_to_archive.append(archive_row.values.tolist())
                rows_to_delete_indices.append(row_num)
                continue

        # -------------------------------------------------------
        # LOGIC 2: SEND FOLLOW-UP
        # -------------------------------------------------------
        if status == "Follow-up" and email and count < MAX_FOLLOWUPS:
            days_since = get_days_diff(last_sent)
            
            # Check if enough days passed (Default: 7)
            if days_since >= DAYS_BETWEEN_SENDS:
                next_stage = count + 1
                template = TEMPLATES.get(next_stage)
                
                if not template:
                    logging.warning(f"No template for stage {next_stage}")
                    continue
                
                # Determine Name
                body = template["body"].format(name="there") 
                
                # Handle Subject (Re:)
                orig_subject = str(row.get("Email Subject", "")).replace("Re: ", "")
                subject = f"Re: {orig_subject}" if orig_subject else "Quick follow up"

                logging.info(f"📧 Sending Follow-up #{next_stage} to {email}...")

                if MODE == "REAL":
                    try:
                        send_real(email, subject, body)
                        
                        # --- SAFE UPDATE USING MAPPED INDICES ---
                        # We use the indices we found at the start of the script
                        ws.update_cell(row_num, idx_sent_at, get_timestamp_iso())
                        ws.update_cell(row_num, idx_count, next_stage)
                        ws.update_cell(row_num, idx_status, "SENT_AUTO")
                        
                        logging.info("✅ Sent & Updated!")
                        time.sleep(2) # Be gentle with API
                        
                    except Exception as e:
                        logging.error(f"❌ Failed to send: {e}")
                else:
                    logging.info(f"[DRYRUN] Would send Follow-up #{next_stage} to {email}")

    # -------------------------------------------------------
    # EXECUTE ARCHIVE
    # -------------------------------------------------------
    if rows_to_archive and MODE == "REAL":
        try:
            try:
                lost_sheet = main_doc.worksheet("Lost_Leads")
            except gspread.WorksheetNotFound:
                lost_sheet = main_doc.add_worksheet(title="Lost_Leads", rows=1000, cols=20)
                lost_sheet.append_row(df.columns.tolist())

            logging.info(f"📦 Moving {len(rows_to_archive)} rows to Lost_Leads...")
            lost_sheet.append_rows(rows_to_archive)
            
            # Delete from main sheet (Reverse order is critical!)
            for idx in sorted(rows_to_delete_indices, reverse=True):
                ws.delete_rows(idx)
                logging.info(f"❌ Deleted row {idx} from active sheet")
                time.sleep(1)

        except Exception as e:
            logging.error(f"Archive failed: {e}")

    logging.info("🏁 Automation Run Complete.")

if __name__ == "__main__":
    process_daily_automation()