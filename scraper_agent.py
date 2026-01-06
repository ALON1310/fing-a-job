import os
import re
import pandas as pd
import time
import redis
import logging
import colorlog
import gspread 
import smtplib 
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email import encoders
from datetime import datetime 
from oauth2client.service_account import ServiceAccountCredentials 
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright
from openai import OpenAI

# --- 1. CONFIGURATION ---
load_dotenv()
# Set the cutoff date for scraping (November 15, 2025)
CUTOFF_DATE = datetime(2025, 11, 15)

# Logging configuration (English)
formatter = colorlog.ColoredFormatter(
    "%(log_color)s%(asctime)s - %(levelname)s - %(message)s",
    datefmt='%H:%M:%S',
    log_colors={
        'DEBUG':    'cyan',
        'INFO':     'green',
        'WARNING':  'yellow',
        'ERROR':    'red',
        'CRITICAL': 'red,bg_white',
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

# --- EMAIL CONFIGURATION ---
EMAIL_SENDER = os.getenv("EMAIL_SENDER")
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD")
# Default recipient if not specified in .env
EMAIL_RECIPIENT = os.getenv("EMAIL_RECIPIENT", "dor@platonics.co") 

# --- HELPER FUNCTIONS ---

def parse_job_date(date_str):
    """Converts string dates like 'Jan 03, 2026' to datetime objects."""
    try:
        clean_date = date_str.replace(",", "").strip()
        return datetime.strptime(clean_date, "%b %d %Y")
    except Exception:
        return datetime.now()

def save_excel_locally(data_list, filename):
    """Saves the current data to Excel (Backup mechanism)."""
    try:
        df = pd.DataFrame(data_list)
        with pd.ExcelWriter(filename, engine='xlsxwriter') as writer:
            df.to_excel(writer, sheet_name='Sales Pipeline', index=False)
            workbook = writer.book
            worksheet = writer.sheets['Sales Pipeline']
            
            # Formatting
            header_fmt = workbook.add_format({
                'bold': True, 'bg_color': '#4472C4', 'font_color': 'white', 
                'border': 1, 'align': 'center', 'valign': 'vcenter', 'font_size': 12
            })
            contact_fmt = workbook.add_format({
                'bg_color': '#FFF2CC', 'border': 1, 'valign': 'top', 'text_wrap': True
            })
            text_wrap_fmt = workbook.add_format({'text_wrap': True, 'valign': 'top'})
            basic_fmt = workbook.add_format({'valign': 'top'})
            
            # Freeze panes
            worksheet.freeze_panes(1, 0) 
            
            # Write Headers
            for col_num, value in enumerate(df.columns.values):
                worksheet.write(0, col_num, value, header_fmt)
            
            # Column Widths
            worksheet.set_column('A:A', 30, basic_fmt)       
            worksheet.set_column('B:C', 15, basic_fmt)       
            worksheet.set_column('D:D', 35, contact_fmt)     
            worksheet.set_column('E:E', 40, basic_fmt)       
            worksheet.set_column('F:F', 50, text_wrap_fmt)   
            worksheet.set_column('G:G', 15, basic_fmt)       
            worksheet.set_column('H:H', 30, basic_fmt)       

        logging.info(f"💾 Checkpoint: Saved {len(data_list)} leads to {filename}")
    except Exception as e:
        logging.error(f"Failed to save local Excel backup: {e}")

def send_email_with_attachment(file_path):
    """Sends the generated Excel report via email."""
    if not EMAIL_SENDER or not EMAIL_PASSWORD:
        logging.warning("Email credentials missing in .env. Skipping email sending.")
        return

    try:
        logging.info(f"Sending report to {EMAIL_RECIPIENT}...")
        
        msg = MIMEMultipart()
        msg['From'] = EMAIL_SENDER
        msg['To'] = EMAIL_RECIPIENT
        msg['Subject'] = f"New Sales Leads Report - {datetime.now().strftime('%d/%m/%Y')}"

        body = "Hello,\n\nAttached is the latest Job Scraper Report containing new leads.\n\nBest regards,\nYour AI Agent"
        
        # --- SAFE UTF-8 ENCODING ---
        msg.attach(MIMEText(body, 'plain', 'utf-8'))

        # Attach the file
        with open(file_path, "rb") as attachment:
            part = MIMEBase("application", "octet-stream")
            part.set_payload(attachment.read())
        
        encoders.encode_base64(part)
        part.add_header("Content-Disposition", f"attachment; filename= {os.path.basename(file_path)}")
        msg.attach(part)

        # Connect to Gmail SMTP
        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls()
        server.login(EMAIL_SENDER, EMAIL_PASSWORD)
        text = msg.as_string()
        server.sendmail(EMAIL_SENDER, EMAIL_RECIPIENT, text)
        server.quit()
        
        logging.info("Email sent successfully!")
    except Exception as e:
        logging.error(f"Failed to send email: {e}")

def update_google_sheets(data_list):
    """Uploads valid leads to the specified Google Sheet in BATCH mode."""
    try:
        if not os.path.exists('credentials.json'):
            logging.warning("credentials.json not found. Skipping Google Sheets update.")
            return

        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name('credentials.json', scope)
        gs_client = gspread.authorize(creds)
        
        sheet = gs_client.open("OJ_Scraper_Leads").sheet1
        
        # Prepare all rows for a single batch upload
        rows_to_upload = []
        for entry in data_list:
            row = [
                entry["Job Title"], 
                entry["Salary"], 
                entry["Post Date"], 
                entry["Contact Info"], 
                entry["Link"], 
                entry["Description"],
                "New", 
                ""     
            ]
            rows_to_upload.append(row)
            
        # Batch upload to avoid API Quota limits
        if rows_to_upload:
            sheet.append_rows(rows_to_upload)
            logging.info(f"Successfully uploaded {len(rows_to_upload)} rows to Google Sheets!")
            
    except Exception as e:
        logging.error(f"Failed to update Google Sheets: {e}")

def extract_contact_with_ai(overview_text):
    """Uses OpenAI to extract hidden contact details from job descriptions."""
    if not overview_text or len(overview_text) < 15:
        return "None"
    
    # NO TRUNCATION: Reading full text to capture details at the end
    logging.info("Speed mode: Waiting 3 seconds...")
    time.sleep(3) 

    # --- STRICT PROMPT FOR ORGANIC LEADS ONLY ---
    system_instruction = """
    You are a strict Headhunter Assistant that EXTRACTS contact information ONLY. 
    Your ONLY goal is to find **DIRECT & ORGANIC** communication channels of the decision-maker (Founder/Hiring Manager).
    
    Data to EXTRACT (Valid - Organic Only):
    1. **Direct Email:** Personal/Business emails. DECODE obfuscated emails (e.g., "john [at] gmail" -> "john@gmail.com").
    2. **Direct Phone/Messaging:** WhatsApp, Telegram, Skype ID.
    3. **Direct Booking Links:** Calendly, SavvyCal, HubSpot Meetings.
    4. **Personal Profiles:** Personal LinkedIn profile URLs (Not company pages).

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
    """Main execution function."""
    logging.info("STARTING BACKGROUND JOB RUN (HEADLESS MODE)...")
    
    # Prepare Filename for Backup
    filename = f"OJ_Sales_Leads_{time.strftime('%Y%m%d_%H%M')}.xlsx"
    
    with sync_playwright() as p:
        user_data_dir = os.path.join(os.getcwd(), "user_data")
        
        # --- HEADLESS MODE ---
        browser_context = p.chromium.launch_persistent_context(
            user_data_dir, 
            headless=True,
            args=["--disable-blink-features=AutomationControlled"]
        )
        
        page = browser_context.pages[0] if browser_context.pages else browser_context.new_page()

        logging.info("PHASE 1: AUTHENTICATION")
        
        # FIX: timeout=0 means "Wait forever until it loads" (Prevents TimeoutError)
        page.goto("https://www.onlinejobs.ph/login", wait_until="domcontentloaded", timeout=0)
        
        # Check if manual login is needed
        if "login" in page.url:
             logging.warning("Login page detected! Since we are in headless mode, I cannot login manually.")
             logging.warning("Please run once with headless=False to save your login session.")
             browser_context.close()
             return

        logging.info("Navigating to job search results...")
        
        # FIX: timeout=0 here as well
        page.goto("https://www.onlinejobs.ph/jobseekers/jobsearch", wait_until="domcontentloaded", timeout=0)
        
        try:
            # Wait for results to appear (timeout=0 means infinite wait)
            page.wait_for_selector('.results', timeout=0)
        except Exception:
             logging.error("Could not load results. Maybe login session expired?")
             browser_context.close()
             return
             
        time.sleep(3)

        results = [] 
        seen_ids = set()
        stop_scraping = False 
        
        # Main Scraping Loop
        while not stop_scraping:
            logging.info(f"Scanning page... Leads collected so far: {len(results)}")
            
            raw_links = page.locator('div.desc a[href*="/jobseekers/job/"]').all()
            if not raw_links:
                logging.warning("⚠️ WARNING: Found 0 links on this page! The site layout might have changed.")

            unique_urls = []
            
            # Deduplication
            for link in raw_links:
                url = link.get_attribute("href")
                if url:
                    match = re.search(r'(\d+)$', url.split('?')[0].rstrip('/'))
                    if match and match.group(1) not in seen_ids:
                        seen_ids.add(match.group(1))
                        unique_urls.append(url)

            logging.info(f"Found {len(unique_urls)} potential leads on this page.")

            for profile_url in unique_urls:
                full_url = "https://www.onlinejobs.ph" + profile_url
                
                # Check Redis Cache
                if r and r.exists(full_url):
                    logging.warning(f"Skipping cached lead: {full_url}")
                    continue

                try:
                    candidate_page = browser_context.new_page()
                    
                    # FIX: timeout=0 -> Wait as long as needed for job page to load
                    candidate_page.goto(full_url, wait_until="domcontentloaded", timeout=0)
                    time.sleep(2)

                    # Date Check
                    date_sel = "body > div > section.bg-ltblue.pt-4.pt-lg-0 > div > div.card.job-post.shadow.mb-4.mb-md-0 > div > div > div:nth-child(4) > dl > dd > p"
                    post_date_str = candidate_page.locator(date_sel).inner_text().strip() if candidate_page.locator(date_sel).count() > 0 else "N/A"
                    
                    job_date = parse_job_date(post_date_str)
                    
                    # Stop if date is too old
                    if job_date < CUTOFF_DATE:
                        logging.warning(f"Reached cutoff date ({post_date_str}). Stopping scraper!")
                        stop_scraping = True
                        candidate_page.close()
                        break 

                    # Extract Data
                    title_sel = "h1"
                    job_title = candidate_page.locator(title_sel).first.inner_text().strip() if candidate_page.locator(title_sel).count() > 0 else "N/A"
                    
                    sal_sel = "body > div > section.bg-ltblue.pt-4.pt-lg-0 > div > div.card.job-post.shadow.mb-4.mb-md-0 > div > div > div:nth-child(2) > dl > dd > p"
                    salary = candidate_page.locator(sal_sel).inner_text().strip() if candidate_page.locator(sal_sel).count() > 0 else "N/A"

                    overview_sel = '//*[@id="job-description"]'
                    overview = candidate_page.locator(overview_sel).inner_text().strip() if candidate_page.locator(overview_sel).count() > 0 else "N/A"

                    logging.info(f"Processing: {job_title} | Date: {post_date_str}")
                    
                    # AI Analysis
                    contact_info = extract_contact_with_ai(overview)

                    # --- THE STRICT "DOUBLE FILTER" ---
                    is_valid = False
                    
                    # Convert contact to lowercase for checking
                    contact_lower = contact_info.lower() if contact_info else ""
                    
                    if contact_info and "none" not in contact_lower and len(contact_info) > 5:
                        
                        # 1. ALLOW LIST (Must contain one of these signals)
                        allow_signals = ["@", "type:", "phone", "whatsapp", "telegram", "signal", "calendly", "savvycal", "hubspot", "linkedin.com/in/", "+"]
                        has_allow_signal = any(sig in contact_lower for sig in allow_signals)
                        
                        # 2. BLOCK LIST (Must NOT contain these)
                        block_signals = ["forms.gle", "docs.google.com/forms", "typeform", "surveymonkey", "bamboohr", "workable", "apply"]
                        has_block_signal = any(sig in contact_lower for sig in block_signals)

                        if has_allow_signal and not has_block_signal:
                            is_valid = True
                    
                    if is_valid:
                        new_lead = {
                            "Job Title": job_title,
                            "Salary": salary,
                            "Post Date": post_date_str,
                            "Contact Info": contact_info,
                            "Link": full_url,
                            "Description": overview,
                            "Status": "New", 
                            "Notes": ""      
                        }
                        results.append(new_lead)
                        if r:
                            r.set(full_url, "processed", ex=2592000) 
                        logging.info(f"✅ Saved Lead! Found: {contact_info} (Total: {len(results)})")

                        # Incremental Backup
                        if len(results) % 5 == 0:
                            save_excel_locally(results, filename)
                    else:
                        logging.warning(f"Discarding (Filtered Output): {contact_info}")

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
            
            # --- PAGINATION LOGIC (FIXED) ---
            logging.info("Moving to NEXT page...")
            try:
                # Robust selection of the next button
                next_btn = page.locator('ul.pagination li a:has-text(">")') 
                
                # Check if button exists AND is visible
                if next_btn.count() > 0 and next_btn.is_visible():
                    # force=True handles cases where a popup covers the button
                    next_btn.click(force=True)
                    logging.info("Clicked Next >")
                    
                    # Wait for load state with infinite timeout
                    try:
                        page.wait_for_load_state("domcontentloaded", timeout=0)
                        time.sleep(3) # Extra small buffer
                    except Exception: 
                        time.sleep(10) # Fallback wait
                else:
                    logging.warning("No 'Next' page found (End of results). Stopping.")
                    break
            except Exception as e:
                logging.error(f"Pagination error: {e}")
                break

        # Final Export & Email
        browser_context.close()
        
        if results:
            save_excel_locally(results, filename) # Final Save
            update_google_sheets(results)
            send_email_with_attachment(filename)
            os.system(f"open '{filename}'")
            logging.info("Done.")

# --- MANUAL EXECUTION ---
if __name__ == "__main__":
    run_job_seeker_agent()