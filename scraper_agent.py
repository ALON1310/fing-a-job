import os
import re
import time
import redis
import logging
import colorlog
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright
from openai import OpenAI

# --- 1. CONFIGURATION ---
load_dotenv()
GOOGLE_SHEET_NAME = "Master_Leads_DB"
DUPLICATE_THRESHOLD = 5  # Stop scraping if we encounter 5 existing leads in a row

# Logging configuration
formatter = colorlog.ColoredFormatter(
    "%(log_color)s%(asctime)s - %(levelname)s - %(message)s",
    datefmt='%H:%M:%S',
    log_colors={
        'DEBUG': 'cyan',
        'INFO': 'green',
        'WARNING': 'yellow',
        'ERROR': 'red',
        'CRITICAL': 'red,bg_white'
    }
)
stream_handler = colorlog.StreamHandler()
stream_handler.setFormatter(formatter)
file_handler = logging.FileHandler("scraper.log")
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))

logger = logging.getLogger()
logger.setLevel(logging.INFO)
if logger.hasHandlers():
    logger.handlers.clear()
logger.addHandler(stream_handler)
logger.addHandler(file_handler)

# --- 2. SETUP OPENAI ---
OPENAI_KEY = os.getenv("OPENAI_API_KEY")
if not OPENAI_KEY:
    logging.error("OPENAI_API_KEY not found in .env file!")
    exit()

client = OpenAI(api_key=OPENAI_KEY)

# --- 3. REDIS SETUP ---
try:
    r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
    r.ping()
    logging.info("Redis connection successful.")
except Exception as e:
    logging.error(f"Could not connect to Redis: {e}. Running without cache.")
    r = None

# --- HELPER FUNCTIONS ---

def is_salary_too_low(salary_str):
    """
    Returns True if salary is below threshold ($5/hr or $500/mo).
    """
    if not salary_str or "negotiable" in salary_str.lower():
        return False
    
    clean_text = salary_str.lower().replace(",", "")
    numbers = re.findall(r'\d+(?:\.\d+)?', clean_text)
    
    if not numbers:
        return False
    
    values = [float(n) for n in numbers]
    max_val = max(values)
    
    if "php" in clean_text or "₱" in clean_text:
        max_val = max_val / 58
        
    if "hour" in clean_text or "hr" in clean_text:
        if max_val < 5:
            return True
    else:
        if max_val < 500:
            return True
            
    return False

def save_to_google_sheets(new_leads):
    """
    Appends new leads to Google Sheets, avoiding duplicates.
    """
    if not new_leads:
        return

    try:
        # Connect to Google Sheets
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name('credentials.json', scope)
        gs_client = gspread.authorize(creds)
        
        sheet = gs_client.open(GOOGLE_SHEET_NAME).sheet1
        
        # Get existing data
        existing_data = sheet.get_all_records()
        existing_links = {str(row.get('Link', '')) for row in existing_data}
        
        # Filter duplicates
        unique_leads = []
        for lead in new_leads:
            if str(lead['Link']) not in existing_links:
                unique_leads.append(lead)
        
        if not unique_leads:
            logging.info("ℹ️ No new unique leads to upload to Google Sheets.")
            return

        # Prepare rows for upload (ensure order matches headers)
        # Note: 'headers' variable removed as it was unused.
        rows_to_upload = []
        
        for lead in unique_leads:
            row = [
                lead.get("Job Title", ""),
                lead.get("Salary", ""),
                lead.get("Post Date", ""),
                lead.get("Contact Info", ""),
                lead.get("Link", ""),
                lead.get("Description", ""),
                lead.get("Status", "New"),      # Default
                lead.get("Sales Rep", "Unassigned"), # Default
                lead.get("Notes", "")
            ]
            rows_to_upload.append(row)

        # Upload
        sheet.append_rows(rows_to_upload)
        logging.info(f"☁️ Successfully uploaded {len(rows_to_upload)} new leads to Google Sheets!")

    except Exception as e:
        logging.error(f"Failed to update Google Sheets: {e}")

def extract_contact_with_ai(overview_text):
    if not overview_text or len(overview_text) < 15:
        return "None"
    
    logging.info("Speed mode: Waiting 3 seconds...")
    time.sleep(3) 

    system_instruction = """
    You are a strict Headhunter Assistant that EXTRACTS contact information ONLY. 
    Your ONLY goal is to find **DIRECT & ORGANIC** communication channels.
    
    Data to EXTRACT (Valid - Organic Only):
    1. **Direct Email:** Personal/Business emails. DECODE obfuscated emails.
    2. **Direct Phone/Messaging:** WhatsApp, Telegram, Skype ID.
    3. **Direct Booking Links:** Calendly, SavvyCal, HubSpot Meetings.
    4. **Personal Profiles:** Personal LinkedIn profile URLs.

    Data to IGNORE (Invalid):
    1. ❌ Passive Forms (Google Forms, Typeform).
    2. ❌ Generic/Agency Portals.
    3. ❌ Generic Career Pages or Support Emails (info@).

    Strict Output Format:
    - If found: "Type: Value | Type: Value"
    - If NOTHING found: "None" (Do not add punctuation).
    """

    try:
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": system_instruction},
                {"role": "user", "content": f"Find contact info in this text:\n{overview_text}"}
            ],
            temperature=0 
        )
        return response.choices[0].message.content.strip()

    except Exception as e:
        logging.error(f"OpenAI API Error: {e}")
        time.sleep(5)
        return "None"

def run_job_seeker_agent():
    logging.info("🚀 STARTING PRODUCTION SCRAPER (Smart Stop Mode)...")
    
    with sync_playwright() as p:
        user_data_dir = os.path.join(os.getcwd(), "user_data")
        browser_context = p.chromium.launch_persistent_context(
            user_data_dir, headless=True, args=["--disable-blink-features=AutomationControlled"]
        )
        page = browser_context.pages[0] if browser_context.pages else browser_context.new_page()

        logging.info("PHASE 1: AUTHENTICATION")
        page.goto("https://www.onlinejobs.ph/login", wait_until="domcontentloaded", timeout=0)
        
        if "login" in page.url:
             logging.warning("Login required! Please run with headless=False once.")
             browser_context.close()
             return

        logging.info("Navigating to job search results...")
        page.goto("https://www.onlinejobs.ph/jobseekers/jobsearch", wait_until="domcontentloaded", timeout=0)
        
        try:
            page.wait_for_selector('.results', timeout=0)
        except Exception:
             logging.error("Could not load results.")
             browser_context.close()
             return
             
        time.sleep(3)

        batch_results = [] 
        seen_ids = set()
        stop_scraping = False 
        consecutive_existing_leads = 0 # Counter for stop condition
        
        while not stop_scraping:
            logging.info(f"Scanning page... Current Batch: {len(batch_results)}")
            
            raw_links = page.locator('div.desc a[href*="/jobseekers/job/"]').all()
            if not raw_links:
                logging.warning("⚠️ WARNING: Found 0 links! The site layout might have changed.")

            unique_urls = []
            for link in raw_links:
                url = link.get_attribute("href")
                if url:
                    match = re.search(r'(\d+)$', url.split('?')[0].rstrip('/'))
                    if match and match.group(1) not in seen_ids:
                        seen_ids.add(match.group(1))
                        unique_urls.append(url)

            logging.info(f"Found {len(unique_urls)} potential leads on this page.")

            if len(unique_urls) == 0:
                 logging.warning("No new unique links on this page. Stopping.")
                 break

            for profile_url in unique_urls:
                full_url = "https://www.onlinejobs.ph" + profile_url
                
                # --- CHECK REDIS (CACHE) FOR STOP CONDITION ---
                if r and r.exists(full_url):
                    consecutive_existing_leads += 1
                    logging.info(f"⏭️ Lead already exists ({consecutive_existing_leads}/{DUPLICATE_THRESHOLD}). Skipping...")
                    
                    if consecutive_existing_leads >= DUPLICATE_THRESHOLD:
                        logging.info(f"🛑 Reached {DUPLICATE_THRESHOLD} existing leads in a row. We are caught up! Stopping.")
                        stop_scraping = True
                        break # Break inner loop
                    
                    continue # Skip this specific lead
                else:
                    consecutive_existing_leads = 0 # Reset counter if we find a new lead
                
                # --- PROCESS NEW LEAD ---
                try:
                    candidate_page = browser_context.new_page()
                    candidate_page.goto(full_url, wait_until="domcontentloaded", timeout=0)
                    time.sleep(2)

                    # Extract basic data
                    date_sel = "body > div > section.bg-ltblue.pt-4.pt-lg-0 > div > div.card.job-post.shadow.mb-4.mb-md-0 > div > div > div:nth-child(4) > dl > dd > p"
                    if candidate_page.locator(date_sel).count() > 0:
                        post_date_str = candidate_page.locator(date_sel).inner_text().strip()
                    else:
                        post_date_str = "N/A"
                    
                    title_sel = "h1"
                    if candidate_page.locator(title_sel).count() > 0:
                        title = candidate_page.locator(title_sel).first.inner_text().strip()
                    else:
                        title = "N/A"

                    sal_sel = "dl > dd > p"
                    if candidate_page.locator(sal_sel).count() > 0:
                        salary = candidate_page.locator(sal_sel).nth(0).inner_text().strip()
                    else:
                        salary = "N/A"
                    
                    # SALARY FILTER CHECK
                    if is_salary_too_low(salary):
                        logging.warning(f"📉 Low Salary: {salary}. Marking processed.")
                        if r:
                            r.set(full_url, "processed", ex=2592000)
                        candidate_page.close()
                        continue
                    
                    desc_sel = '//*[@id="job-description"]'
                    if candidate_page.locator(desc_sel).count() > 0:
                        desc = candidate_page.locator(desc_sel).inner_text().strip()
                    else:
                        desc = "N/A"

                    logging.info(f"Processing: {title} | Salary: {salary}")
                    
                    contact_info = extract_contact_with_ai(desc)

                    # DOUBLE FILTER VALIDATION
                    is_valid = False
                    contact_lower = contact_info.lower() if contact_info else ""
                    
                    if contact_info and "none" not in contact_lower and len(contact_info) > 5:
                        allow_signals = ["@", "type:", "phone", "whatsapp", "telegram", "signal", "calendly", "savvycal", "hubspot", "linkedin.com/in/", "+"]
                        has_allow_signal = any(sig in contact_lower for sig in allow_signals)
                        block_signals = ["forms.gle", "docs.google.com/forms", "typeform", "surveymonkey", "bamboohr", "workable", "apply"]
                        has_block_signal = any(sig in contact_lower for sig in block_signals)

                        if has_allow_signal and not has_block_signal:
                            is_valid = True
                    
                    if is_valid:
                        new_lead = {
                            "Job Title": title, 
                            "Salary": salary, 
                            "Post Date": post_date_str, 
                            "Contact Info": contact_info, 
                            "Link": full_url, 
                            "Description": desc,
                            "Status": "New", 
                            "Sales Rep": "Unassigned", 
                            "Notes": ""
                        }
                        batch_results.append(new_lead)
                        # Important: Mark as processed in Redis so next time we know to stop here
                        if r:
                            r.set(full_url, "processed", ex=2592000)
                        logging.info(f"✅ Lead Found! {contact_info}")

                        # Save every 3 leads
                        if len(batch_results) >= 3:
                            save_to_google_sheets(batch_results)
                            batch_results = [] 
                    else:
                        logging.warning(f"Discarding: {contact_info}")
                        # Even if discarded (no contact info), mark as processed so we don't scan it again
                        if r:
                            r.set(full_url, "processed", ex=2592000)

                    candidate_page.close()

                except Exception as e:
                    logging.error(f"Error processing {full_url}: {e}")
                    try:
                        candidate_page.close() 
                    except Exception:
                        pass
                    continue
            
            if stop_scraping:
                break
            
            # PAGINATION
            logging.info("Moving to NEXT page...")
            try:
                next_btn = page.locator('ul.pagination li a:has-text(">")') 
                if next_btn.count() > 0 and next_btn.is_visible():
                    next_btn.click(force=True)
                    try:
                        page.wait_for_load_state("domcontentloaded", timeout=0)
                        time.sleep(3)
                    except Exception:
                        time.sleep(10)
                else:
                    logging.info("No 'Next' button found. End of list.")
                    stop_scraping = True
                    break
            except Exception: 
                stop_scraping = True
                break

        browser_context.close()
        
        # Save any remaining leads
        if batch_results:
            save_to_google_sheets(batch_results)
            
        logging.info("🎉 Scraper Finished Successfully.")

if __name__ == "__main__":
    run_job_seeker_agent()