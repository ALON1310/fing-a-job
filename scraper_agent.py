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
DUPLICATE_THRESHOLD = 50  # Scans 50 duplicate jobs before stopping

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

# Clear existing handlers to prevent duplicates
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
    """Downloads all existing links from Google Sheets to memory to prevent duplicates."""
    try:
        client = get_sheet_client()
        sheet = client.open(GOOGLE_SHEET_NAME).sheet1
        
        records = sheet.get_all_values()
        
        # Assuming the Link is in Column E (index 4 in 0-based list)
        # Skip header row and extract column 4
        links = {row[4] for row in records[1:] if len(row) > 4}
        
        logging.info(f"📚 Loaded {len(links)} existing leads from Google Sheets.")
        return links
    except Exception as e:
        logging.warning(f"Could not load existing links: {e}")
        return set()


# --- HELPER FUNCTIONS ---

def is_salary_too_low(salary_str):
    """Checks if the salary is below the minimum threshold."""
    if not salary_str or "negotiable" in salary_str.lower():
        return False
    
    clean = salary_str.lower().replace(",", "")
    nums = re.findall(r'\d+(?:\.\d+)?', clean)
    
    if not nums:
        return False
    
    val = max([float(n) for n in nums])
    
    # Adjust for currency (PHP)
    if "php" in clean or "₱" in clean:
        val /= 58
        
    # Check hourly vs monthly
    if "hour" in clean or "hr" in clean:
        return val < 5
    return val < 500


def extract_contact_with_ai(text):
    """Uses OpenAI to find direct contact information."""
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
    """Appends new leads to the Google Sheet."""
    if not new_leads:
        return
    
    try:
        client = get_sheet_client()
        sheet = client.open(GOOGLE_SHEET_NAME).sheet1
        
        # Prepare rows for upload
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
    
    with sync_playwright() as p:
        # Launch browser options for cloud environment
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
            
            if "login" in page.url:
                user = os.getenv("JOB_USERNAME")
                pwd = os.getenv("JOB_PASSWORD")
                
                if not user or not pwd:
                    logging.error("❌ Missing Username/Password secrets!")
                    return
                
                page.fill("input[name='email']", user)
                page.fill("input[type='password']", pwd)
                page.click("button[type='submit']")
                
                try:
                    page.wait_for_url("**/jobseekers/*", timeout=30000)
                    logging.info("✅ Login Success!")
                except Exception:
                    logging.error("❌ Login Failed/Timeout.")
                    page.screenshot(path="login_failed.png")
                    return

            # 2. SEARCH
            logging.info("Step 2: Scanning Jobs...")
            page.goto("https://www.onlinejobs.ph/jobseekers/jobsearch", timeout=60000)
            time.sleep(5)
            page.screenshot(path="search_page.png")

            batch = []
            consecutive_dupes = 0
            
            # Scan up to 3 pages or until stopped
            for page_num in range(3): 
                links = page.locator('div.desc a[href*="/jobseekers/job/"]').all()
                logging.info(f"Page {page_num+1}: Found {len(links)} links.")
                
                if not links:
                    page.screenshot(path=f"empty_page_{page_num}.png")
                    break

                for link in links:
                    if consecutive_dupes >= DUPLICATE_THRESHOLD:
                        logging.info("🛑 Threshold reached. Stopping.")
                        break
                    
                    url = "https://www.onlinejobs.ph" + link.get_attribute("href")
                    
                    # Check against Google Sheets links
                    if url in existing_db_links:
                        consecutive_dupes += 1
                        print(".", end="", flush=True)  # Print dot for duplicate
                        continue
                    
                    consecutive_dupes = 0  # Reset counter once a new item is found
                    logging.info(f"\n🔍 Scanning New: {url}")
                    
                    try:
                        p2 = context.new_page()
                        p2.goto(url, timeout=30000)
                        
                        desc = p2.locator('#job-description').inner_text()
                        
                        # Handle potential missing elements gracefully
                        salary_element = p2.locator("dl > dd > p").first
                        salary = salary_element.inner_text() if salary_element.count() > 0 else "N/A"
                        
                        title_element = p2.locator("h1").first
                        title = title_element.inner_text() if title_element.count() > 0 else "N/A"
                        
                        # Quick filter based on salary
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
                            # Add to local memory to avoid rescanning in this run
                            existing_db_links.add(url)
                        
                        p2.close()
                        
                        # Save in small batches
                        if len(batch) >= 2:
                            save_to_google_sheets(batch)
                            batch = []
                            
                    except Exception as e:
                        logging.error(f"Link Error: {e}")

                if consecutive_dupes >= DUPLICATE_THRESHOLD:
                    break
                
                # Pagination: Go to next page
                try:
                    next_btn = page.locator('ul.pagination li a:has-text(">")')
                    if next_btn.count() > 0:
                        next_btn.click()
                        time.sleep(3)
                    else:
                        break
                except Exception:
                    break

            # Save any remaining leads
            if batch:
                save_to_google_sheets(batch)

        except Exception as e:
            logging.error(f"CRITICAL: {e}")
            page.screenshot(path="error.png")
        finally:
            browser.close()


if __name__ == "__main__":
    run_job_seeker_agent()