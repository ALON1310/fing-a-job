#!/usr/bin/env python3
"""
maintenance_tool.py (OPTIMIZED)

1. Deletes rows with no contact info.
2. Fixes corrupted 'Followup Count' (removes text/drafts).
3. Resets stuck statuses (PENDING/FAILED) so they can be retried.
4. Enriches missing drafts using AI.

Optimized to use Batch Updates to save API quotas.
"""

from __future__ import annotations

import logging
import os
import time
from typing import Any

from dotenv import load_dotenv

from scraper_agent import extract_data_with_ai, generate_email_body
from sheets_client import get_sheet_client
from utils import colnum_to_a1, setup_logging

# --- CONFIGURATION ---
load_dotenv()
SHEET_NAME = os.getenv("SHEET", "Master_Leads_DB").strip()

setup_logging()


def clean_and_enrich_db() -> None:
    logging.info("🚀 STARTING MAINTENANCE AGENT (Smart Clean, Fix & Enrich)")
    logging.info("------------------------------------------------")

    try:
        client = get_sheet_client()
        ws = client.open(SHEET_NAME).sheet1
    except Exception as e:
        logging.error(f"❌ Connection error: {e}")
        return

    logging.info("📥 Downloading sheet data...")
    try:
        all_values = ws.get_all_values()
    except Exception as e:
        logging.error(f"❌ Failed to fetch sheet: {e}")
        return

    if len(all_values) < 2:
        logging.warning("⚠️ Sheet is empty.")
        return

    headers = all_values[0]

    # ---------------------------------------------------------
    # PHASE 1: COLUMN MAPPING
    # ---------------------------------------------------------
    def get_col_idx(name: str) -> int:
        return headers.index(name) if name in headers else -1

    idx_contact = get_col_idx("Contact Info")
    idx_desc = get_col_idx("Description")
    idx_title = get_col_idx("Job Title")
    idx_draft = get_col_idx("Draft Email")
    idx_subject = get_col_idx("Email Subject")

    # Status & Tracking Columns
    idx_status = get_col_idx("Status")
    idx_send_status = get_col_idx("Send Status")
    idx_mode = get_col_idx("Send Mode")
    idx_error = get_col_idx("Last Error")
    idx_attempts = get_col_idx("Send Attempts")
    idx_followup = get_col_idx("Followup Count")

    if idx_contact == -1:
        logging.error("❌ 'Contact Info' column missing.")
        return

    # ---------------------------------------------------------
    # PHASE 2: DELETION (Must be done first, backwards)
    # ---------------------------------------------------------
    rows_to_delete: list[int] = []

    logging.info("🗑️ Phase 1: Checking for empty contacts...")

    # Loop backwards so deletion doesn't mess up indices
    for i in reversed(range(1, len(all_values))):
        row_data = all_values[i]
        actual_row_num = i + 1

        contact_val = row_data[idx_contact].strip() if len(row_data) > idx_contact else ""

        if (not contact_val) or (contact_val.lower() == "none"):
            logging.info(f"   Row {actual_row_num}: Marking for DELETE (No contact)")
            rows_to_delete.append(actual_row_num)

    if rows_to_delete:
        logging.info(f"⚡ Deleting {len(rows_to_delete)} rows...")
        for row_num in rows_to_delete:
            try:
                ws.delete_rows(row_num)
                time.sleep(1.0)  # Safety sleep
            except Exception as e:
                logging.error(f"   Delete failed: {e}")

        logging.info("🔄 Re-fetching data after deletion...")
        try:
            all_values = ws.get_all_values()
        except Exception as e:
            logging.error(f"❌ Failed to re-fetch after deletion: {e}")
            return

        if len(all_values) < 2:
            logging.warning("⚠️ Sheet became empty after deletions.")
            return

        # Refresh headers + indices after deletion (safe, even if unchanged)
        headers = all_values[0]

        def get_col_idx_refreshed(name: str) -> int:
            return headers.index(name) if name in headers else -1

        idx_desc = get_col_idx_refreshed("Description")
        idx_title = get_col_idx_refreshed("Job Title")
        idx_draft = get_col_idx_refreshed("Draft Email")
        idx_subject = get_col_idx_refreshed("Email Subject")
        idx_status = get_col_idx_refreshed("Status")
        idx_send_status = get_col_idx_refreshed("Send Status")
        idx_mode = get_col_idx_refreshed("Send Mode")
        idx_error = get_col_idx_refreshed("Last Error")
        idx_attempts = get_col_idx_refreshed("Send Attempts")
        idx_followup = get_col_idx_refreshed("Followup Count")
        idx_contact = get_col_idx_refreshed("Contact Info")

    # ---------------------------------------------------------
    # PHASE 3: REPAIRS & RESETS (Batch Operation)
    # ---------------------------------------------------------
    logging.info("🔧 Phase 2: Fixing Columns & Resetting Statuses...")

    batch_updates: list[dict[str, Any]] = []

    for i, row in enumerate(all_values):
        if i == 0:
            continue  # Skip header

        row_num = i + 1

        # A. FIX FOLLOWUP COUNT (The Bug Fix)
        if idx_followup != -1 and len(row) > idx_followup:
            val = row[idx_followup]
            looks_corrupt = (
                (len(val) > 5)
                or ("@" in val)
                or ("Hi " in val)
                or (not val.isdigit())
            )
            if looks_corrupt:
                current_status = row[idx_status] if (idx_status != -1 and len(row) > idx_status) else ""

                # Logic: If lead is advanced, assume at least 1, else 0
                new_val = "1" if current_status in ["Follow-up", "In Progress"] else "0"

                logging.warning(
                    f"   Row {row_num}: Fixing Followup Count ('{val[:10]}...' -> '{new_val}')"
                )
                cell_a1 = f"{colnum_to_a1(idx_followup + 1)}{row_num}"
                batch_updates.append({"range": cell_a1, "values": [[new_val]]})

        # B. SMART RESET (Clear PENDING/FAILED/ERROR)
        # Only if NOT Sent/Skipped/Manual
        status_val = ""
        if idx_send_status != -1 and len(row) > idx_send_status:
            status_val = row[idx_send_status]

        if status_val not in ["SENT", "MANUAL_CHECK", "SKIPPED"]:
            needs_clean = False

            if idx_mode != -1 and len(row) > idx_mode and row[idx_mode]:
                needs_clean = True
            if idx_error != -1 and len(row) > idx_error and row[idx_error]:
                needs_clean = True
            if status_val:
                needs_clean = True  # PENDING / FAILED / etc.

            if needs_clean:
                logging.info(f"   Row {row_num}: Resetting status for retry")

                if idx_send_status != -1:
                    batch_updates.append(
                        {"range": f"{colnum_to_a1(idx_send_status + 1)}{row_num}", "values": [[""]]}
                    )
                if idx_mode != -1:
                    batch_updates.append(
                        {"range": f"{colnum_to_a1(idx_mode + 1)}{row_num}", "values": [[""]]}
                    )
                if idx_error != -1:
                    batch_updates.append(
                        {"range": f"{colnum_to_a1(idx_error + 1)}{row_num}", "values": [[""]]}
                    )
                if idx_attempts != -1:
                    batch_updates.append(
                        {"range": f"{colnum_to_a1(idx_attempts + 1)}{row_num}", "values": [["0"]]}
                    )

    if batch_updates:
        logging.info(f"💾 Saving {len(batch_updates)} repairs/resets to Sheets...")
        try:
            chunk_size = 50
            for k in range(0, len(batch_updates), chunk_size):
                ws.batch_update(batch_updates[k : k + chunk_size])
                time.sleep(1.0)
            logging.info("✅ Batch update complete.")
        except Exception as e:
            logging.error(f"❌ Batch update failed: {e}")

    # ---------------------------------------------------------
    # PHASE 4: AI ENRICHMENT (Sequential / slow)
    # ---------------------------------------------------------
    if idx_draft == -1:
        logging.info("🏁 Done (Skipping AI: No Draft Column)")
        return

    logging.info("🧠 Phase 3: AI Enrichment (Generating missing drafts)...")

    try:
        all_values = ws.get_all_values()
    except Exception as e:
        logging.error(f"❌ Failed to fetch before AI enrichment: {e}")
        return

    generated_count = 0

    for i, row in enumerate(all_values):
        if i == 0:
            continue

        row_num = i + 1
        draft_val = row[idx_draft] if len(row) > idx_draft else ""

        # Only generate if draft is EMPTY
        if not draft_val:
            title = row[idx_title] if (idx_title != -1 and len(row) > idx_title) else "Role"
            desc = row[idx_desc] if (idx_desc != -1 and len(row) > idx_desc) else ""

            if len(desc) > 20:
                logging.info(f"   Row {row_num}: Generating AI Draft for '{title}'...")
                try:
                    ai_data = extract_data_with_ai(desc, title)
                    generated_hook = ai_data.get("hook", "")

                    new_draft = generate_email_body(
                        first_name=ai_data.get("name", "there"),
                        role_name=title,
                        hook=generated_hook,
                    )
                    new_subject = f"Quick question about your {title} role"

                    ws.update_cell(row_num, idx_draft + 1, new_draft)
                    if idx_subject != -1:
                        ws.update_cell(row_num, idx_subject + 1, new_subject)

                    generated_count += 1
                    time.sleep(1.5)
                except Exception as e:
                    logging.error(f"   ⚠️ Generation failed: {e}")

    logging.info("------------------------------------------------")
    logging.info(f"🏁 MAINTENANCE COMPLETE. Generated {generated_count} new drafts.")
    logging.info("------------------------------------------------")


if __name__ == "__main__":
    clean_and_enrich_db()
