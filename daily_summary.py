#!/usr/bin/env python3
"""
daily_summary.py
Sends a daily recap email to the management team.
"""

import os
import smtplib
import logging
from datetime import datetime
from email.message import EmailMessage
from dotenv import load_dotenv
import pandas as pd

from sheets_client import get_sheet_client
from utils import setup_logging

# --- CONFIGURATION ---
load_dotenv()
setup_logging()

SHEET_NAME = os.getenv("SHEET", "Master_Leads_DB")

# Defined Distribution List
RECIPIENTS = [
    "jane@platonics.co",
    "jessica@platonics.co",
    "genecortes@platonics.co",
    "dor@platonics.co"
]

# SMTP Server Details (Loaded from ENV)
SMTP_HOST = os.getenv("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER = os.getenv("SMTP_USER", "")
SMTP_PASS = os.getenv("SMTP_PASS", "")
MAIL_FROM = os.getenv("MAIL_FROM", SMTP_USER)

def send_daily_summary():
    logging.info("📧 Preparing Daily Summary Email...")

    # 1. Fetch Data
    try:
        client = get_sheet_client()
        ws = client.open(SHEET_NAME).sheet1
        data = ws.get_all_records()
        df = pd.DataFrame(data)
    except Exception as e:
        logging.error(f"❌ Failed to fetch data from sheets: {e}")
        return

    if df.empty:
        logging.info("⚠️ No data in sheet.")
        return

    # 2. Filter "Today's" Leads
    # Note: This assumes the scraper ran today and dates match the format (e.g., "Jan 20 2026").
    # We filter based on "Post Date" matching today's date string.
    
    today_str = datetime.now().strftime("%b %d %Y") # e.g., "Jan 20 2026"
    
    # Filter logic
    if "Post Date" in df.columns:
        # Filter rows where the date column contains today's date string
        todays_leads = df[df["Post Date"].astype(str).str.contains(today_str, case=False, na=False)]
    else:
        # Fallback: If no date column exists, take the last 20 rows (Not recommended)
        todays_leads = df.tail(20)

    total_new = len(todays_leads)
    
    # 3. Analysis: Who has phone vs. email
    # Assumption: If it contains '@', it is an email; otherwise, it is a phone number.
    
    with_email = todays_leads[todays_leads["Contact Info"].astype(str).str.contains("@", na=False)]
    email_count = len(with_email)
    
    # All others are considered "Phone" (unless empty, but we follow the request logic)
    with_phone = todays_leads[~todays_leads["Contact Info"].astype(str).str.contains("@", na=False)]
    phone_count = len(with_phone)

    logging.info(f"📊 Summary: Total={total_new}, Email={email_count}, Phone={phone_count}")

    # 4. Build Email Content
    subject = f"📢 Daily Leads Update - {datetime.now().strftime('%d/%m/%Y')}"
    
    body = f"""
    Hi Team,
    
    Here is the summary for today ({datetime.now().strftime('%d/%m/%Y')}):
    
    🚀 <b>Total New Leads Found: {total_new}</b>
    
    📞 <b>{phone_count} leads have phone numbers.</b>
    Please contact them today.
    
    ✉️ <b>{email_count} leads have no phone (Email only).</b>
    Initial outreach email has been sent automatically.
    
    ---
    Keep crushing it! 💪
    Platonics Bot
    """

    # 5. Send Email to Recipients
    try:
        msg = EmailMessage()
        msg["From"] = MAIL_FROM
        msg["To"] = ", ".join(RECIPIENTS) # Send to all in the 'To' field
        msg["Subject"] = subject
        msg.set_content(body)
        msg.add_alternative(body.replace("\n", "<br>"), subtype='html') # HTML version

        with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=30) as server:
            server.ehlo()
            server.starttls()
            server.ehlo()
            server.login(SMTP_USER, SMTP_PASS)
            server.send_message(msg)
            
        logging.info(f"✅ Daily summary sent successfully to {len(RECIPIENTS)} recipients.")

    except Exception as e:
        logging.error(f"❌ Failed to send summary email: {e}")

if __name__ == "__main__":
    send_daily_summary()