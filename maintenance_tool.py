#!/usr/bin/env python3
"""
maintenance_tool.py (REFACTORED - Connected to utils.py)

Logic remains identical.
- Uses shared logging from utils.py.
- Maintains AI enrichment and Smart Reset logic.
"""

import os
import time
import logging
from dotenv import load_dotenv
from sheets_client import get_sheet_client
from scraper_agent import extract_data_with_ai, generate_email_body

# --- NEW: Import shared tools ---
from utils import setup_logging

# --- CONFIGURATION ---
load_dotenv()
SHEET_NAME = os.getenv("SHEET", "Master_Leads_DB")  

# --- LOGGING SETUP ---
# Use the shared logging setup from utils.py
setup_logging()

def clean_and_enrich_db():
    logging.info("🚀 STARTING MAINTENANCE AGENT (Smart Clean & Reset)")
    logging.info("------------------------------------------------")
    
    try:
        # 1. Connect
        client = get_sheet_client()
        sheet = client.open(SHEET_NAME).sheet1
        
        logging.info("📥 Downloading sheet data...")
        all_values = sheet.get_all_values()
        
        if len(all_values) < 2:
            logging.warning("⚠️ Sheet is empty.")
            return

        headers = all_values[0]
        
        # 2. Map Columns
        try:
            col_map = {name.strip(): i for i, name in enumerate(headers)}
            
            # Data Columns
            idx_contact = col_map["Contact Info"]
            idx_desc = col_map["Description"]
            idx_title = col_map["Job Title"]
            idx_draft = col_map.get("Draft Email") 
            idx_subject = col_map.get("Email Subject")
            
            # Status Columns
            idx_status = col_map.get("Send Status")
            idx_mode = col_map.get("Send Mode")
            idx_error = col_map.get("Last Error")
            idx_attempts = col_map.get("Send Attempts")

            if idx_draft is None:
                logging.error("❌ 'Draft Email' column missing.")
                return

        except KeyError as e:
            logging.error(f"❌ Missing critical column: {e}")
            return

    except Exception as e:
        logging.error(f"❌ Connection error: {e}")
        return

    logging.info(f"📚 Scanning {len(all_values) - 1} rows...")
    logging.info("------------------------------------------------")

    deleted_count = 0
    updated_count = 0
    cleaned_count = 0
    skipped_protected_count = 0

    # 🔄 Loop Backwards
    for i in reversed(range(1, len(all_values))):
        row_data = all_values[i]
        actual_row_num = i + 1  
        
        # Safe Get Contact
        contact_info = row_data[idx_contact].strip() if len(row_data) > idx_contact else ""
        
        # --- 1. DELETE LOGIC (Rows with no contact) ---
        if not contact_info or contact_info.lower() == "none":
            logging.info(f"🗑️  Row {actual_row_num}: DELETE -> No contact.")
            try:
                sheet.delete_rows(actual_row_num)
                deleted_count += 1
                time.sleep(1.5) 
            except Exception as e:
                logging.error(f"   ❌ Delete failed: {e}")
                time.sleep(5)
            continue 

        # --- 2. ENRICH LOGIC (Fill missing drafts) ---
        current_draft = row_data[idx_draft].strip() if len(row_data) > idx_draft else ""
        
        if not current_draft:
            title = row_data[idx_title] if len(row_data) > idx_title else "Role"
            description = row_data[idx_desc] if len(row_data) > idx_desc else ""
            
            logging.info(f"⚡ Row {actual_row_num}: GENERATING DRAFT -> '{title}'")
            
            if len(description) > 20:
                try:
                    # AI Analysis
                    logging.info("   🧠 AI Generating Hook...")
                    ai_data = extract_data_with_ai(description, title)
                    generated_hook = ai_data.get("hook", "")
                    
                    # Generate Email
                    new_draft = generate_email_body(
                        first_name=ai_data.get("name", "there"),
                        role_name=title,
                        hook=generated_hook
                    )
                    new_subject = f"Quick question about your {title} role"
                    
                    # Update Content
                    sheet.update_cell(actual_row_num, idx_draft + 1, new_draft)
                    if idx_subject is not None:
                         sheet.update_cell(actual_row_num, idx_subject + 1, new_subject)

                    updated_count += 1
                    time.sleep(2.0) 
                    
                except Exception as e:
                    logging.error(f"   ⚠️ Error generating draft: {e}")
                    time.sleep(5)

        # --- 3. SMART RESET LOGIC (Protect SENT rows) ---
        
        curr_status = row_data[idx_status].strip() if idx_status and len(row_data) > idx_status else ""
        curr_mode = row_data[idx_mode].strip() if idx_mode and len(row_data) > idx_mode else ""
        curr_error = row_data[idx_error].strip() if idx_error and len(row_data) > idx_error else ""
        curr_attempts = row_data[idx_attempts].strip() if idx_attempts and len(row_data) > idx_attempts else ""

        if curr_status in ["SENT", "MANUAL_CHECK", "SKIPPED"]:
            skipped_protected_count += 1
            continue 


        if curr_status or curr_mode or curr_error or (curr_attempts and curr_attempts != "0"):
            logging.info(f"   🧹 Wiping status (Resetting for retry) for Row {actual_row_num}...")
            try:
                if idx_status is not None:
                    sheet.update_cell(actual_row_num, idx_status + 1, "")
                    time.sleep(0.8)
                
                if idx_mode is not None:
                    sheet.update_cell(actual_row_num, idx_mode + 1, "")
                    time.sleep(0.8)
                
                if idx_error is not None:
                    sheet.update_cell(actual_row_num, idx_error + 1, "")
                    time.sleep(0.8)
                    
                if idx_attempts is not None:
                    sheet.update_cell(actual_row_num, idx_attempts + 1, "")
                    time.sleep(0.8)
                    
                cleaned_count += 1
            except Exception as e:
                logging.error(f"   ❌ Failed to wipe row {actual_row_num}: {e}")
                if "429" in str(e):
                    logging.warning("   ⏳ Sleeping 30s due to rate limit...")
                    time.sleep(30)

    logging.info("------------------------------------------------")
    logging.info(f"🏁 DONE: Deleted {deleted_count} | Generated {updated_count} | Cleaned {cleaned_count} | Protected {skipped_protected_count}")
    logging.info("------------------------------------------------")

if __name__ == "__main__":
    clean_and_enrich_db()