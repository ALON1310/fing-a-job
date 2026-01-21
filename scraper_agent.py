import os
import re
import time
import json
import logging
from datetime import datetime
from typing import Dict, Any, Set, List, Callable, Tuple, Optional

from dotenv import load_dotenv
from openai import OpenAI
from playwright.sync_api import sync_playwright
from schedule import logger
from sheets_client import get_sheet_client

# --- NEW: Import shared tools ---
from utils import setup_logging
import salary_parser


# -----------------------------
# 1) CONFIGURATION
# -----------------------------

# ✅ FIX: Load .env BEFORE reading os.getenv values
load_dotenv()

MIN_MONTHLY_USD = float(os.getenv("MIN_MONTHLY_USD", "900"))
MIN_MONTHLY_PHP = float(os.getenv("MIN_MONTHLY_PHP", "50000"))
UNKNOWN_POLICY = os.getenv("UNKNOWN_SALARY_POLICY", "keep").strip().lower()

# UPDATED: Matches your .env "SHEET"
GOOGLE_SHEET_NAME = os.getenv("SHEET", "Master_Leads_DB")
DUPLICATE_THRESHOLD = int(os.getenv("DUPLICATE_THRESHOLD", "50"))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "2"))
HEADLESS = os.getenv("HEADLESS", "1").strip().lower() not in ("0", "false", "no")
DEBUG_SAVE_ALL = os.getenv("DEBUG_SAVE_ALL", "0").strip().lower() in ("1", "true", "yes")

# Logging options
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()


# -----------------------------
# 2) LOGGING SETUP
# -----------------------------
setup_logging()


# -----------------------------
# 3) CLIENT SETUP
# -----------------------------

OPENAI_KEY = os.getenv("OPENAI_API_KEY")
client = OpenAI(api_key=OPENAI_KEY) if OPENAI_KEY else None

if not OPENAI_KEY:
    logging.warning("⚠️ OPENAI_API_KEY is not set. AI extraction will use fallback defaults.")


def with_retry(
    fn: Callable[[], Any],
    attempts: int = 3,
    delay_sec: float = 2.0,
    allowed_exceptions: Tuple[type, ...] = (Exception,),
) -> Any:
    """Run a function with retry logic."""
    last_err: Optional[Exception] = None
    for i in range(attempts):
        try:
            return fn()
        except allowed_exceptions as e:
            last_err = e
            logging.warning(f"Retry {i + 1}/{attempts} failed: {e}")
            if i < attempts - 1:
                time.sleep(delay_sec)
    raise last_err


# -----------------------------
# 4) HELPERS (Google Sheets & URL)
# -----------------------------

def canonical_job_url(url: str) -> str:
    """Normalize OLJ job URLs to avoid duplicates (strip extra text, keep ID)."""
    m = re.search(r"(\d+)(?:\D*$)", url)
    if not m:
        return url
    job_id = m.group(1)
    return f"https://www.onlinejobs.ph/jobseekers/job/{job_id}"


def get_existing_links() -> Set[str]:
    """Download existing links from Google Sheets to memory."""
    try:
        gs_client = get_sheet_client()
        sheet = gs_client.open(GOOGLE_SHEET_NAME).sheet1
        records = sheet.get_all_values()
        # Extract from column E (index 4)
        links_raw = {row[4] for row in records[1:] if len(row) > 4 and row[4]}
        links = {canonical_job_url(x.strip()) for x in links_raw if x.strip()}
        logging.info(f"📚 Loaded {len(links)} existing leads from Google Sheets.")
        return links
    except Exception as e:
        logging.warning(f"Could not load existing links: {e}")
        return set()


def save_to_google_sheets(new_leads: List[Dict[str, Any]]) -> None:
    """Append new leads to Google Sheets with CORRECT column alignment."""
    if not new_leads:
        return
    logging.info(f"🧾 Saving {len(new_leads)} leads to Cloud...")
    try:
        gs_client = get_sheet_client()
        sheet = gs_client.open(GOOGLE_SHEET_NAME).sheet1
        rows = []
        for lead in new_leads:
            rows.append([
                lead.get("Job Title", ""),       # A
                lead.get("Salary", ""),          # B
                lead.get("Post Date", ""),       # C
                lead.get("Contact Info", ""),    # D
                lead.get("Link", ""),            # E
                lead.get("Description", ""),     # F
                "New",                           # G
                "Unassigned",                    # H
                lead.get("Notes", ""),           # I
                "",                              # J (Send Mode)
                "",                              # K (Send Status)
                "",                              # L (Send Attempts)
                "",                              # M (Last Error)
                "",                              # N (Last Sent At)
                lead.get("Draft Email", ""),     # O (Draft Email)
                lead.get("Email Subject", "")    # P (Email Subject)
            ])
        sheet.append_rows(rows)
        logging.info(f"✅ Successfully uploaded {len(rows)} leads.")
    except Exception as e:
        logging.error(f"❌ Sheet Error: {e}")


def normalize_contact(contact: object) -> str:
    if contact is None:
        return "None"
    if isinstance(contact, dict):
        candidates = [str(v).strip() for v in contact.values() if v and str(v).lower() != "none"]
        return candidates[0] if candidates else "None"
    s = str(contact).strip()
    return s if s else "None"


def has_real_contact(contact_str: str) -> bool:
    if not contact_str:
        return False
    c = contact_str.strip()
    if c.lower() == "none":
        return False
    if re.search(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}", c, flags=re.I):
        return True
    if "t.me/" in c.lower():
        return True
    if re.search(r"\+?\d[\d\s\-\(\)]{7,}\d", c):
        return True
    if re.fullmatch(r"@[\w.]{3,}", c):
        return True
    return False


def extract_data_with_ai(job_description: str, job_title: str) -> Dict[str, Any]:
    if not client or not job_description or len(job_description) < 15:
        return {"contact": "None", "name": "there", "hook": ""}

    def _call_openai() -> Dict[str, Any]:
        prompt = (
            f"Analyze this job description for a '{job_title}' role.\n"
            "1) Extract DIRECT contact info (Email, Phone, Telegram, IG). If none, 'None'.\n"
            "2) Extract First Name of hiring manager. If none, 'there'.\n"
            "3) Write 1 personalized sentence based on technical requirements.\n"
            "Return JSON: {'contact': ..., 'name': ..., 'hook': ...}"
        )
        res = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "Assistant. Output valid JSON."},
                {"role": "user", "content": f"{prompt}\n\nJOB DESCRIPTION:\n{job_description}"},
            ],
            temperature=0.3,
            response_format={"type": "json_object"},
        )
        content = (res.choices[0].message.content or "").strip()
        return json.loads(content)

    try:
        return with_retry(_call_openai, attempts=2, delay_sec=1.0)
    except Exception as e:
        logging.error(f"AI Error: {e}")
        return {"contact": "None", "name": "there", "hook": ""}


def generate_email_body(first_name: str, role_name: str, hook: str) -> str:
    return f"""Hi {first_name},

I recently came across your job post for a {role_name} and noticed you're looking to hire from the Philippines.

{hook}

At Platonics, we specialize in solving exactly this challenge. Instead of sifting through dozens of applications yourself, we do the heavy lifting.

We filter and verify candidates to present you with only the top 2-3 profiles that perfectly match your requirements. Plus, we handle everything else: background checks, taxes, benefits, and performance monitoring.

The best part? No recruitment fees or hidden charges. Just one simple monthly payment based on your budget.

I’d love to hop on a brief 15-min discovery call to hear about your specific needs:
👉 https://meetings-na2.hubspot.com/gene-mc

Best regards,
Platonics Team
www.platonics.co
"""


# -----------------------------
# 6) MAIN AGENT
# -----------------------------

def run_job_seeker_agent() -> None:
    logging.info("🚀 STARTING CLOUD SCRAPER WITH EMAIL GEN...")

    existing_db_links = get_existing_links()
    last_processed_url = "None"

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=HEADLESS, args=["--no-sandbox"])
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
        page = context.new_page()

        try:
            # 1) LOGIN
            logging.info("Step 1: Logging in...")
            page.goto("https://www.onlinejobs.ph/login", timeout=60000)

            if page.locator("#login_username").count() > 0:
                user = os.getenv("OJ_EMAIL")
                pwd = os.getenv("OJ_PASSWORD")

                if not user or not pwd:
                    logging.error("❌ Missing OJ_EMAIL/OJ_PASSWORD in .env")
                    return

                page.fill("#login_username", user)
                page.fill("#login_password", pwd)
                page.click("button[type='submit']")
                logging.info("⏳ Login submitted...")
                time.sleep(5)

                try:
                    popup = page.locator("button").filter(has_text="Okay, got it")
                    if popup.count() > 0 and popup.first.is_visible():
                        logging.info("🚧 Popup detected! Clicking 'Okay'...")
                        popup.first.click()
                        time.sleep(2)
                except Exception:
                    pass

            # 2) NAVIGATION
            logging.info("Step 2: Navigating to Job Board...")
            search_url = "https://www.onlinejobs.ph/jobseekers/jobsearch"
            page.goto(search_url, timeout=60000)
            
            try:
                page.wait_for_selector(".jobpost-cat-box, .job-post-snippet", timeout=20000)
                logger.info("✅ Landed on Job Search page successfully.")
            except Exception:
                logger.warning("⚠️ Might not be on the search page yet, attempting to force search...")

            # 3) SCANNING LOOP
            batch: List[Dict[str, Any]] = []
            global_consecutive_dupes = 0

            for page_num in range(50):
                elements = page.locator('div.desc a[href*="/jobseekers/job/"]').all()
                logging.info(f"Page {page_num + 1}: Found {len(elements)} raw links.")

                if not elements:
                    logging.warning("No links found. End of list.")
                    break

                unique_page_urls = []
                seen_on_page = set()

                for el in elements:
                    href = el.get_attribute("href")
                    if href:
                        full_url = "https://www.onlinejobs.ph" + href
                        clean_url = canonical_job_url(full_url)
                        if clean_url not in seen_on_page:
                            seen_on_page.add(clean_url)
                            unique_page_urls.append(clean_url)

                logging.info(f"🧹 Unique jobs on page: {len(unique_page_urls)}")

                for url in unique_page_urls:
                    if global_consecutive_dupes >= DUPLICATE_THRESHOLD:
                        logging.info("🛑 Duplicate threshold reached. Stopping.")
                        break

                    last_processed_url = url

                    if url in existing_db_links:
                        global_consecutive_dupes += 1
                        logging.debug(f"Duplicate in DB: {url}")
                        continue

                    global_consecutive_dupes = 0
                    logging.info(f"\n🔍 Scanning New: {url}")

                    try:
                        p2 = context.new_page()
                        p2.goto(url, timeout=30000)

                        if p2.locator("#job-description").count() == 0:
                            p2.close()
                            continue

                        desc = p2.locator("#job-description").inner_text() or ""

                        salary = "N/A"
                        salary_el = p2.locator("dt:has-text('Salary') + dd")
                        
                        if salary_el.count() > 0:
                            salary = salary_el.first.inner_text().strip()
                        
                        elif p2.locator("div:nth-child(2) > dl > dd > p").count() > 0:
                            salary = p2.locator("div:nth-child(2) > dl > dd > p").first.inner_text().strip()

                        logging.info(f"   💰 Raw Salary Found: '{salary}'")


                        title = "N/A"
                        if p2.locator("h1").count() > 0:
                            title = p2.locator("h1").first.inner_text() or "N/A"

                        # ✅ FIX: Use salary_parser module with thresholds + policy
                        if salary_parser.is_salary_too_low(
                            salary,
                            MIN_MONTHLY_USD,
                            MIN_MONTHLY_PHP,
                            unknown_policy=UNKNOWN_POLICY,
                        ):
                            logging.info("⛔ Salary too low -> skipped.")
                            p2.close()
                            continue

                        ai_data = extract_data_with_ai(desc, title)
                        raw_contact = ai_data.get("contact", "None")
                        contact = normalize_contact(raw_contact)

                        should_save = DEBUG_SAVE_ALL or has_real_contact(contact)

                        if not should_save:
                            logging.info("⛔ Lead SKIPPED (no direct contact).")
                            p2.close()
                            continue

                        notes = "DEBUG_SAVE_ALL" if (DEBUG_SAVE_ALL and not has_real_contact(contact)) else ""

                        draft_email = generate_email_body(
                            first_name=str(ai_data.get("name", "there") or "there"),
                            role_name=title,
                            hook=str(ai_data.get("hook", "") or ""),
                        )

                        email_subject = f"Top Filipino VA for your {title}"

                        batch.append({
                            "Job Title": title,
                            "Salary": salary,
                            "Post Date": datetime.now().strftime("%b %d %Y"),
                            "Contact Info": contact,
                            "Link": url,
                            "Description": desc,
                            "Draft Email": draft_email,
                            "Email Subject": email_subject,
                            "Notes": notes,
                        })

                        existing_db_links.add(url)
                        p2.close()

                        if len(batch) >= BATCH_SIZE:
                            save_to_google_sheets(batch)
                            batch = []

                    except Exception as e:
                        logging.error(f"Link Error: {e}")

                if global_consecutive_dupes >= DUPLICATE_THRESHOLD:
                    break

                try:
                    next_btn = page.locator('ul.pagination li a:has-text(">")')
                    if next_btn.count() > 0 and next_btn.is_visible():
                        logging.info("➡️ Clicking Next Page...")
                        next_btn.click()
                        time.sleep(2)
                    else:
                        logging.info("🛑 No 'Next' button found. End.")
                        break
                except Exception:
                    break

            if batch:
                save_to_google_sheets(batch)

        except Exception as e:
            logging.error(f"CRITICAL: {e}")
        finally:
            browser.close()
            logging.info(f"🏁 FINAL STATUS: Last Lead: {last_processed_url}")


if __name__ == "__main__":
    run_job_seeker_agent()
