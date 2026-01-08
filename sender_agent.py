#!/usr/bin/env python3
"""
sender_agent.py (REFACTORED - Connected to utils.py)

Logic remains identical, but helper functions are now imported from utils.py.
"""

from __future__ import annotations

import logging
import os
import smtplib
import time
import re
from email.message import EmailMessage
from typing import Dict, List, Tuple

from dotenv import load_dotenv
from sheets_client import get_sheet_client

# --- NEW: Import shared tools ---
from utils import (
    setup_logging, 
    get_timestamp_iso, 
    extract_email, 
    colnum_to_a1, 
    ensure_columns
)

# -------------------- ENV --------------------
load_dotenv()

SHEET_NAME = os.getenv("SHEET", "Master_Leads_DB").strip()

VERIFY_ONLY = os.getenv("VERIFY_ONLY", "0").strip() == "1"
MODE = os.getenv("MODE", "DRYRUN").strip().upper()

# Limits
SEND_LIMIT = int(os.getenv("SEND_LIMIT", "500"))
SLEEP_BETWEEN_SENDS_SEC = float(os.getenv("SLEEP_BETWEEN_SENDS_SEC", "0.7"))

# Safety & Batching
BATCH_SAVE_SIZE = 10
BATCH_SLEEP_SEC = 2.0

MAX_RETRIES = int(os.getenv("MAX_RETRIES", "2"))
RETRY_SLEEP_SEC = float(os.getenv("RETRY_SLEEP_SEC", "1.0"))

# SMTP
SMTP_HOST = os.getenv("SMTP_HOST", "smtp.gmail.com").strip()
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER = os.getenv("SMTP_USER", "").strip()
SMTP_PASS = os.getenv("SMTP_PASS", "").strip()
MAIL_FROM = os.getenv("MAIL_FROM", SMTP_USER).strip()
MAIL_BCC = os.getenv("MAIL_BCC", "").strip()


# -------------------- LOGGING --------------------
# Use the shared logging setup
setup_logging()


# -------------------- LOCAL HELPERS --------------------
# (These remain here because they are specific to Sheets logic)

def get_cell(row: List[str], headers: List[str], col: str) -> str:
    try:
        idx = headers.index(col)
        return row[idx] if idx < len(row) else ""
    except ValueError:
        return ""


def set_cell(row: List[str], headers: List[str], col: str, value: str) -> None:
    idx = headers.index(col)
    while len(row) <= idx:
        row.append("")
    row[idx] = value


def default_subject_from_row(headers: List[str], row: List[str]) -> str:
    title = get_cell(row, headers, "Job Title").strip()
    if title:
        return f"Quick question about your {title} role"
    return "Quick question about your hiring needs"


def resolve_body(headers: List[str], row: List[str]) -> Tuple[str, str]:
    def normalize_header(h: str) -> str:
        return re.sub(r"\s+", " ", (h or "").strip())

    norm_to_original: Dict[str, str] = {}
    for h in headers:
        nh = normalize_header(h)
        if nh and nh not in norm_to_original:
            norm_to_original[nh] = h

    def get_by_contract_name(contract_name: str) -> str:
        original = norm_to_original.get(contract_name)
        if not original:
            return ""
        return get_cell(row, headers, original).strip()

    draft = get_by_contract_name("Draft Email")
    if draft:
        return (draft, "Draft Email")

    return ("", "MISSING")


# -------------------- SENDERS --------------------
def send_mock(to_email: str, subject: str, body: str) -> None:
    _ = body
    logging.info(f"[MOCK] Sent to {to_email} | Subject: {subject}")


def send_real(to_email: str, subject: str, body: str) -> None:
    if not SMTP_USER or not SMTP_PASS or not MAIL_FROM:
        raise RuntimeError("SMTP credentials missing.")

    msg = EmailMessage()
    msg["From"] = MAIL_FROM
    msg["To"] = to_email
    msg["Subject"] = subject
    if MAIL_BCC:
        msg["Bcc"] = MAIL_BCC

    msg.set_content(body)

    with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=30) as server:
        server.ehlo()
        server.starttls()
        server.ehlo()
        server.login(SMTP_USER, SMTP_PASS)
        server.send_message(msg)


# -------------------- MAIN --------------------
def run_sender_agent() -> None:
    logging.info(f"Sender starting | MODE={MODE} | VERIFY_ONLY={VERIFY_ONLY} | SHEET='{SHEET_NAME}'")

    ws = get_sheet_client().open(SHEET_NAME).sheet1
    all_values = ws.get_all_values()

    if not all_values or len(all_values) < 2:
        logging.info("No data found in sheet.")
        return

    # Normalize headers
    def normalize_header(h: str) -> str:
        return re.sub(r"\s+", " ", (h or "").strip())

    raw_headers = all_values[0]
    headers_norm = [normalize_header(h) for h in raw_headers]
    data_rows = all_values[1:]
    headers = list(headers_norm)

    # Ensure tracking columns (Using UTILS function)
    tracking_cols = ["Send Mode", "Send Status", "Send Attempts", "Last Error", "Last Sent At"]
    new_headers = ensure_columns(headers, tracking_cols)

    if new_headers != headers:
        if VERIFY_ONLY:
            logging.info("[VERIFY_ONLY] Missing tracking cols. Skipping write.")
            headers = new_headers
        else:
            ws.update(values=[new_headers], range_name="1:1")
            headers = new_headers
        headers_norm = list(headers)

    # Required Columns
    required_cols = ["Contact Info", "Draft Email", "Email Subject"]
    missing = [c for c in required_cols if c not in headers_norm]
    if missing:
        raise RuntimeError(f"Missing required columns: {missing}")

    sent_count = 0
    batch_ranges: List[Dict[str, object]] = []
    rows_processed_in_batch = 0

    logging.info(f"🔎 Scanning {len(data_rows)} rows... (Batch Size: {BATCH_SAVE_SIZE})")

    for row_idx, row in enumerate(data_rows, start=2):
        while len(row) < len(headers_norm):
            row.append("")

        # 1. Check existing Send Status
        send_status = get_cell(row, headers_norm, "Send Status").strip().upper()
        
        # Skip if handled
        if send_status in ("SENT", "SKIPPED", "MANUAL_CHECK", "PENDING"):
            if send_status == "PENDING" and MODE == "REAL":
                pass 
            else:
                continue

        # --- CRM SAFETY CHECK ---
        crm_status = get_cell(row, headers_norm, "Status").strip().lower()
        if crm_status not in ("", "new"):
            set_cell(row, headers_norm, "Send Status", "SKIPPED")
            set_cell(row, headers_norm, "Last Error", f"Skipped due to Status: {crm_status}")
            
            if not VERIFY_ONLY:
                # Using UTILS colnum_to_a1
                last_col = colnum_to_a1(len(headers_norm))
                range_name = f"A{row_idx}:{last_col}{row_idx}"
                batch_ranges.append({"range": range_name, "values": [row]})
            continue

        if not VERIFY_ONLY and sent_count >= SEND_LIMIT:
            logging.info(f"🛑 Reached global SEND_LIMIT={SEND_LIMIT}. Stopping loop.")
            break

        contact_raw = get_cell(row, headers_norm, "Contact Info").strip()
        
        # Using UTILS extract_email
        to_email = extract_email(contact_raw)
        
        # --- LOGIC START ---
        
        # 1. Check if email exists
        if not to_email:
            is_empty = (not contact_raw) or (contact_raw.lower() == "none")
            
            if is_empty:
                if VERIFY_ONLY:
                    logging.info(f"[VERIFY_ONLY] Row {row_idx}: SKIPPED (No contact info)")
                else:
                    set_cell(row, headers_norm, "Send Status", "SKIPPED")
                    set_cell(row, headers_norm, "Last Error", "No contact info found")
            else:
                if VERIFY_ONLY:
                    logging.info(f"[VERIFY_ONLY] Row {row_idx}: MANUAL_CHECK (Contact: {contact_raw})")
                else:
                    set_cell(row, headers_norm, "Send Status", "MANUAL_CHECK")
                    set_cell(row, headers_norm, "Last Error", "Non-email contact found")
            
            if not VERIFY_ONLY:
                last_col = colnum_to_a1(len(headers_norm))
                range_name = f"A{row_idx}:{last_col}{row_idx}"
                batch_ranges.append({"range": range_name, "values": [row]})
                rows_processed_in_batch += 1
        
        else:
            # 2. Email Exists
            body, body_source = resolve_body(headers_norm, row)

            if not body:
                if VERIFY_ONLY:
                    logging.info(f"[VERIFY_ONLY] Row {row_idx}: FAILED (Missing Draft)")
                else:
                    set_cell(row, headers_norm, "Send Mode", MODE)
                    set_cell(row, headers_norm, "Send Status", "FAILED")
                    set_cell(row, headers_norm, "Last Error", "Missing Draft Email")
                    
                    last_col = colnum_to_a1(len(headers_norm))
                    range_name = f"A{row_idx}:{last_col}{row_idx}"
                    batch_ranges.append({"range": range_name, "values": [row]})
                    rows_processed_in_batch += 1
            
            else:
                subject = get_cell(row, headers_norm, "Email Subject").strip()
                if not subject:
                    subject = default_subject_from_row(headers_norm, row)

                if VERIFY_ONLY:
                    logging.info(f"[VERIFY_ONLY] Row {row_idx}: READY ({to_email})")
                
                else:
                    # EXECUTE SEND
                    set_cell(row, headers_norm, "Send Mode", MODE)
                    
                    if MODE == "DRYRUN":
                        set_cell(row, headers_norm, "Send Status", "PENDING")
                        set_cell(row, headers_norm, "Last Error", "")
                        logging.info(f"[DRYRUN] Row {row_idx}: Marked PENDING ({to_email})")
                    else:
                        attempts_str = get_cell(row, headers_norm, "Send Attempts").strip()
                        attempts = int(attempts_str) if attempts_str.isdigit() else 0
                        
                        ok = False
                        last_err = ""

                        for attempt_idx in range(1, MAX_RETRIES + 1):
                            try:
                                attempts += 1
                                set_cell(row, headers_norm, "Send Attempts", str(attempts))

                                if MODE == "MOCK":
                                    send_mock(to_email, subject, body)
                                elif MODE == "REAL":
                                    send_real(to_email, subject, body)
                                
                                ok = True
                                break
                            except Exception as e:
                                last_err = str(e)
                                logging.warning(f"Retry {attempt_idx}/{MAX_RETRIES} failed: {last_err}")
                                time.sleep(RETRY_SLEEP_SEC)

                        if ok:
                            set_cell(row, headers_norm, "Send Status", "SENT")
                            set_cell(row, headers_norm, "Last Error", "")
                            # Using UTILS get_timestamp_iso
                            set_cell(row, headers_norm, "Last Sent At", get_timestamp_iso())
                            sent_count += 1
                            time.sleep(SLEEP_BETWEEN_SENDS_SEC)
                        else:
                            set_cell(row, headers_norm, "Send Status", "FAILED")
                            set_cell(row, headers_norm, "Last Error", last_err)

                    last_col = colnum_to_a1(len(headers_norm))
                    range_name = f"A{row_idx}:{last_col}{row_idx}"
                    batch_ranges.append({"range": range_name, "values": [row]})
                    rows_processed_in_batch += 1

        # --- BATCH SAVE LOGIC ---
        if not VERIFY_ONLY and len(batch_ranges) >= BATCH_SAVE_SIZE:
            logging.info(f"💾 Saving batch of {len(batch_ranges)} rows to Sheets...")
            try:
                ws.batch_update(batch_ranges)
                batch_ranges = []
                rows_processed_in_batch = 0
                logging.info(f"☕ Pausing for {BATCH_SLEEP_SEC}s to respect quotas...")
                time.sleep(BATCH_SLEEP_SEC)
            except Exception as e:
                logging.error(f"❌ Batch Save Failed: {e}")

    # Final Save
    if not VERIFY_ONLY and batch_ranges:
        logging.info(f"💾 Saving final {len(batch_ranges)} rows...")
        ws.batch_update(batch_ranges)

    logging.info("🏁 Done.")

if __name__ == "__main__":
    run_sender_agent()