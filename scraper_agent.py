import os
import re
import time
import logging
from datetime import datetime

import colorlog
import gspread
from dotenv import load_dotenv
from oauth2client.service_account import ServiceAccountCredentials
from openai import OpenAI
from playwright.sync_api import sync_playwright

# --- 1. CONFIGURATION ---
load_dotenv()
GOOGLE_SHEET_NAME = "Master_Leads_DB"
DUPLICATE_THRESHOLD = 50  # Stop scraping after finding 50 duplicate jobs in a row

# Logging configuration
formatter = colorlog.ColoredFormatter(
    "%(log_color)s%(asctime)s - %(levelname)s - %(message)s",
    datefmt='%H:%M:%S',
    log_colors={
        'DEBUG': 'cyan',
        'INFO': 'green',
        'WARNING': 'yellow',
        'ERROR': 'red'
    }
)
stream_handler = colorlog.StreamHandler()
stream_handler.setFormatter(formatter)

logger = logging.getLogger()
logger.setLevel(logging.INFO)

if logger.hasHandlers():
    logger.handlers.clear()
logger.addHandler(stream_handler)


# --- 2. SETUP CLIENTS ---
OPENAI_KEY = os.getenv("OPENAI_API_KEY")
client = OpenAI(api_key=OPENAI_KEY) if OPENAI_KEY else None


def get_sheet_client():
    """Establishes connection to Google Sheets."""
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive"
    ]
    creds = ServiceAccountCredentials.from_json_keyfile_name(
        'credentials.json', scope
    )
    return gspread.authorize(creds)


def get_existing_links():
    """Downloads all existing links from Google Sheets to memory."""
    try:
        client = get_sheet_client()
        sheet = client.open(GOOGLE_SHEET_NAME).sheet1
        records = sheet.get_all_values()
        # Extract links from column E (index 4)
        links = {row[4] for row in records[1:] if len(row) > 4}
        logging.info(f"📚 Loaded {len(links)} existing leads from Google Sheets.")
        return links
    except Exception as e:
        logging.warning(f"Could not load existing links: {e}")
        return set()


# --- HELPER FUNCTIONS ---

def is_salary_too_low(salary_str):
    """Checks if salary is below threshold."""
    if not salary_str or "negotiable" in salary_str.lower():
        return False
    clean = salary_str.lower().replace(",", "")
    nums = re.findall(r'\d+(?:\.\d+)?', clean)
    if not nums:
        return False
    val = max([float(n) for n in nums])
    if "php" in clean or "₱" in clean:
        val /= 58
    if "hour" in clean or "hr" in clean:
        return val < 5
    return val < 500


def extract_contact_with_ai(text):
    """Extracts direct contact info using OpenAI."""
    if not client or len(text) < 15:
        return "None"
    try:
        res = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {
                    "role": "system",
                    "content": (
                        "Extract DIRECT contact info (Email, Phone, WhatsApp, Telegram). "
                        "Output: 'Type: Value'. If none, output 'None'."
                    )
                },
                {"role": "user", "content": text}
            ],
            temperature=0
        )
        return res.choices[0].message.content.strip()
    except Exception as e:
        logging.error(f"AI Extraction Error: {e}")
        return "None"


def save_to_google_sheets(new_leads):
    """Appends new leads to Google Sheets."""
    if not new_leads:
        return
    try:
        client = get_sheet_client()
        sheet = client.open(GOOGLE_SHEET_NAME).sheet1
        rows = [
            [
                lead["Job Title"],
                lead["Salary"],
                lead["Post Date"],
                lead["Contact Info"],
                lead["Link"],
                lead["Description"],
                "New",
                "Unassigned",
                ""
            ]
            for lead in new_leads
        ]
        sheet.append_rows(rows)
        logging.info(f"☁️ Uploaded {len(rows)} leads.")
    except Exception as e:
        logging.error(f"Sheet Error: {e}")


# --- MAIN AGENT ---

def run_job_seeker_agent():
    logging.info("🚀 STARTING CLOUD SCRAPER...")
    existing_db_links = get_existing_links()
    last_processed_url = "None"  # Tracks the very last URL touched
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=["--no-sandbox"])
        context = browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            )
        )
        page = context.new_page()

        try:
            # 1. LOGIN
            logging.info("Step 1: Logging in...")
            page.goto("https://www.onlinejobs.ph/login", timeout=60000)
            
            if page.locator("#login_username").count() > 0:
                user = os.getenv("JOB_USERNAME")
                pwd = os.getenv("JOB_PASSWORD")
                
                if not user or not pwd:
                    logging.error("❌ Missing Username/Password secrets!")
                    return
                
                logging.info("📝 Filling credentials...")
                try:
                    page.fill("#login_username", user)
                    page.fill("#login_password", pwd)
                    page.click("button[type='submit']")
                    
                    logging.info("⏳ Waiting for login redirection...")
                    time.sleep(5)
                    
                    try:
                        popup_btn = page.locator("button").filter(has_text="Okay, got it")
                        if popup_btn.count() > 0 and popup_btn.first.is_visible():
                            logging.info("🚧 Popup detected! Clicking 'Okay'...")
                            popup_btn.first.click()
                            time.sleep(3)
                    except Exception:
                        pass
                        
                except Exception as e:
                    logging.error(f"❌ Login Action Failed: {e}")
                    page.screenshot(path="login_failed_action.png")
                    return

            # 2. HUMAN NAVIGATION
            logging.info("Step 2: Navigating via Buttons (Human Mode)...")
            
            try:
                logging.info("🖱️ Clicking 'JOB BOARD'...")
                page.locator('a[href="https://www.onlinejobs.ph/jobs"]').first.click()
                page.wait_for_load_state("domcontentloaded")
                time.sleep(3)
                
                logging.info("🖱️ Clicking 'VIEW MORE JOB POSTS'...")
                page.locator('a[href="/jobseekers/jobsearch"]').first.click()
                page.wait_for_load_state("domcontentloaded")
                time.sleep(3)
                
                if "jobsearch" not in page.url:
                    logging.warning(f"⚠️ Unexpected URL: {page.url}")
                    page.screenshot(path="navigation_failed.png")
                else:
                    logging.info("✅ Successfully arrived at Job Search page!")

            except Exception as e:
                logging.error(f"❌ Navigation Failed: {e}")
                page.screenshot(path="nav_error.png")
                return

            # 3. SCANNING LOOP
            batch = []
            consecutive_dupes = 0
            
            # Increased page limit to 50 to ensure "Run until the end" if needed
            for page_num in range(50): 
                links = page.locator('div.desc a[href*="/jobseekers/job/"]').all()
                logging.info(f"Page {page_num+1}: Found {len(links)} links.")
                
                if not links:
                    logging.warning("No links found on this page. End of list.")
                    page.screenshot(path=f"empty_page_{page_num}.png")
                    break

                for link in links:
                    # Check stop condition (Hit the wall of existing leads)
                    if consecutive_dupes >= DUPLICATE_THRESHOLD:
                        logging.info("🛑 Threshold reached (Found 50 consecutive duplicates). Stopping.")
                        break
                    
                    url = "https://www.onlinejobs.ph" + link.get_attribute("href")
                    
                    # --- UPDATE TRACKING VARIABLE ---
                    last_processed_url = url
                    # --------------------------------
                    
                    if url in existing_db_links:
                        consecutive_dupes += 1
                        print(".", end="", flush=True)
                        continue
                    
                    consecutive_dupes = 0 
                    logging.info(f"\n🔍 Scanning New: {url}")
                    
                    try:
                        p2 = context.new_page()
                        p2.goto(url, timeout=30000)
                        
                        desc_el = p2.locator('#job-description')
                        if desc_el.count() == 0:
                            p2.close()
                            continue
                            
                        desc = desc_el.inner_text()
                        
                        salary = "N/A"
                        if p2.locator("dl > dd > p").count() > 0:
                            salary = p2.locator("dl > dd > p").first.inner_text()
                            
                        title = "N/A"
                        if p2.locator("h1").count() > 0:
                            title = p2.locator("h1").first.inner_text()
                        
                        if is_salary_too_low(salary):
                            p2.close()
                            continue
                            
                        contact = extract_contact_with_ai(desc)
                        
                        if "None" not in contact and len(contact) > 5:
                            logging.info(f"💎 HIT: {contact}")
                            batch.append({
                                "Job Title": title,
                                "Salary": salary,
                                "Post Date": datetime.now().strftime("%b %d %Y"),
                                "Contact Info": contact,
                                "Link": url,
                                "Description": desc
                            })
                            existing_db_links.add(url)
                        
                        p2.close()
                        
                        if len(batch) >= 2:
                            save_to_google_sheets(batch)
                            batch = []
                            
                    except Exception as e:
                        logging.error(f"Link Error: {e}")

                if consecutive_dupes >= DUPLICATE_THRESHOLD:
                    break
                
                # Pagination
                try:
                    next_btn = page.locator('ul.pagination li a:has-text(">")')
                    if next_btn.count() > 0 and next_btn.is_visible():
                        logging.info("➡️ Clicking Next Page...")
                        next_btn.click()
                        time.sleep(3)
                    else:
                        logging.info("🛑 No 'Next' button found. End of scraping.")
                        break
                except Exception:
                    logging.warning("End of pagination reached.")
                    break

            if batch:
                save_to_google_sheets(batch)

        except Exception as e:
            logging.error(f"CRITICAL: {e}")
            page.screenshot(path="error.png")
        finally:
            browser.close()
            logging.info(f"🏁 FINAL STATUS: Last Processed Lead: {last_processed_url}")


if __name__ == "__main__":
    run_job_seeker_agent()