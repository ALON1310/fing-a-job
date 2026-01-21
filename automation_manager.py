#!/usr/bin/env python3
"""
automation_manager.py (DYNAMIC SCHEDULE)
Daily Logic:
1. Archive leads with 5 sent emails (Initial + 4 followups) to 'Lost_Leads'.
2. Send scheduled follow-ups based on specific day intervals.
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
MAX_FOLLOWUPS = 5  # Total emails = Initial (1) + 4 Followups = 5
MODE = os.getenv("MODE", "DRYRUN").upper()

# --- SCHEDULE CONFIGURATION (The Logic You Requested) ---
# Key = Current 'Followup Count' in DB
# Value = Days to wait before sending the NEXT email
# דוגמה: אם הסטטוס הוא 1 (נשלח רק הראשון), מחכים 2 ימים ושולחים את פולואפ מס' 2.
WAIT_SCHEDULE = {
    1: 2,  # After Initial Email -> Wait 2 days -> Send Follow-up 1
    2: 5,  # After Follow-up 1   -> Wait 5 days -> Send Follow-up 2
    3: 7,  # After Follow-up 2   -> Wait 7 days -> Send Follow-up 3
    4: 9   # After Follow-up 3   -> Wait 9 days -> Send Follow-up 4 (Breakup)
}

# --- TEMPLATES ---
# Note: {name} = Client Name, {job} = Job Title
TEMPLATES = {
    2: {
        "subject": "Your {job} hire made simple",
        "body": "Hi {name},\n\nJust checking in! We specialize in experienced Filipino VAs who integrate smoothly and deliver results you can rely on—without the high cost of full-time local hires.\n\nWould you like a quick chat to see if we can help with your {job} role?\n\nBest regards,\nAlon"
    },
    3: {
        "subject": "Proven VA support for {job}",
        "body": "Hi {name},\n\nCompanies in your industry are hiring Filipino VAs with strong domain expertise and seeing reliable results at a fraction of the cost.\n\nI can show you how we tailor VAs specifically for your {job} role and co-manage them to ensure smooth performance.\n\nWould you like a 10–15 minute chat?\n\nBest regards,\nAlon"
    },
    4: {
        "subject": "Still looking for a {job} VA?",
        "body": "Hi {name},\n\nJust checking if you’re still hiring for {job}. We help companies secure skilled, affordable Filipino VAs with proven experience in their field, fully managed for reliability.\n\nEven a short call can show how we can make this easy for you.\n\nThanks,\nAlon"
    },
    5: {
        "subject": "Closing the loop on {job}",
        "body": "Hi {name},\n\nSince I haven't heard back, I assume you’ve likely filled the {job} position or put it on hold.\n\nI won’t fill up your inbox any further, but please keep us in mind if you ever need a hand finding top-tier Filipino VAs in the future—fully managed and reliable.\n\nAll the best,\nAlon"
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
        
        # Ensure columns exist
        for col in ["Followup Count", "Last Sent At", "Status", "Contact Info", "Job Title"]:
            if col not in df.columns:
                df[col] = ""

        # --- Safe Column Mapping ---
        header_row = ws.row_values(1)
        try:
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
        job_title = str(row["Job Title"]).strip() or "your role"
        
        try:
            raw_count = row["Followup Count"]
            if raw_count == "" or raw_count is None:
                count = 0
            else:
                count = int(raw_count)
        except Exception:
            count = 0
            
        email = extract_email(contact)

        # -------------------------------------------------------
        # LOGIC 1: ARCHIVE LOST LEADS (After 5 emails total)
        # -------------------------------------------------------
        if status == "Follow-up" and count >= MAX_FOLLOWUPS:
            days_since = get_days_diff(last_sent)
            
            # Archive only if 3 days passed since the last email (break-up email)
            if days_since >= 3: 
                logging.info(f"🗑️ Archiving Row {row_num} (Max followups reached)")
                
                archive_row = row.copy()
                archive_row["Status"] = "Lost"
                archive_row["Notes"] = f"{row.get('Notes', '')} | Auto-archived after 5 attempts"
                
                rows_to_archive.append(archive_row.values.tolist())
                rows_to_delete_indices.append(row_num)
                continue

        # -------------------------------------------------------
        # LOGIC 2: SEND SCHEDULED FOLLOW-UP
        # -------------------------------------------------------
        if status == "Follow-up" and email and count < MAX_FOLLOWUPS:
            days_since = get_days_diff(last_sent)
            
            # --- DYNAMIC SCHEDULE CHECK ---
            required_wait_days = WAIT_SCHEDULE.get(count, 7) # Default to 7 if logic fails
            
            if days_since >= required_wait_days:
                next_stage = count + 1
                template = TEMPLATES.get(next_stage)
                
                if not template:
                    logging.warning(f"No template for stage {next_stage}")
                    continue
                
                # Prepare content
                try:
                    body = template["body"].format(name="there", job=job_title)
                    subject = template["subject"].format(job=job_title)
                except KeyError as e:
                    logging.error(f"Template Error row {row_num}: {e}")
                    continue

                logging.info(f"📧 Sending Follow-up #{next_stage} to {email} (Waited {days_since} days) | Subject: {subject}")

                if MODE == "REAL":
                    try:
                        send_real(email, subject, body)
                        
                        ws.update_cell(row_num, idx_sent_at, get_timestamp_iso())
                        ws.update_cell(row_num, idx_count, next_stage)
                        ws.update_cell(row_num, idx_status, "SENT_AUTO")
                        
                        logging.info("✅ Sent & Updated!")
                        time.sleep(2) 
                        
                    except Exception as e:
                        logging.error(f"❌ Failed to send: {e}")
                else:
                    logging.info(f"[DRYRUN] Would send Follow-up #{next_stage}")

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
            
            for idx in sorted(rows_to_delete_indices, reverse=True):
                ws.delete_rows(idx)
                logging.info(f"❌ Deleted row {idx} from active sheet")
                time.sleep(1)

        except Exception as e:
            logging.error(f"Archive failed: {e}")

    logging.info("🏁 Automation Run Complete.")

if __name__ == "__main__":
    process_daily_automation()