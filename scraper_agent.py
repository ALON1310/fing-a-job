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
CUTOFF_DATE = datetime(2025, 11, 15)

# Logging configuration
formatter = colorlog.ColoredFormatter(
    "%(log_color)s%(asctime)s - %(levelname)s - %(message)s",
    datefmt='%H:%M:%S',
    log_colors={'DEBUG': 'cyan', 'INFO': 'green', 'WARNING': 'yellow', 'ERROR': 'red', 'CRITICAL': 'red,bg_white'}
)
stream_handler = colorlog.StreamHandler()
stream_handler.setFormatter(formatter)
file_handler = logging.FileHandler("scraper.log")
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logger = logging.getLogger()
logger.setLevel(logging.INFO)
if logger.hasHandlers(): logger.handlers.clear()
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
EMAIL_RECIPIENT = os.getenv("EMAIL_RECIPIENT", "dor@platonics.co") 

# --- HELPER FUNCTIONS ---

def parse_job_date(date_str):
    try:
        clean_date = date_str.replace(",", "").strip()
        return datetime.strptime(clean_date, "%b %d %Y")
    except Exception:
        return datetime.now()

def is_salary_too_low(salary_str):
    """
    Returns True if salary is below threshold ($5/hr or $500/mo).
    Handles PHP to USD conversion automatically.
    """
    if not salary_str or "negotiable" in salary_str.lower():
        return False # Keep if unknown
    
    # Clean string
    clean_text = salary_str.lower().replace(",", "")
    
    # Extract all numbers
    numbers = re.findall(r'\d+(?:\.\d+)?', clean_text)
    if not numbers:
        return False # Keep if no numbers found
    
    # Take the MAXIMUM number found (optimistic check)
    # E.g., if range is "300 - 600", we take 600.
    values = [float(n) for n in numbers]
    max_val = max(values)
    
    # Currency Conversion (Approx 58 PHP = 1 USD)
    if "php" in clean_text or "₱" in clean_text:
        max_val = max_val / 58
        
    # Check Thresholds
    # Case A: Hourly Rate (Look for 'hr' or 'hour')
    if "hour" in clean_text or "hr" in clean_text:
        if max_val < 5:
            return True # FILTER OUT: Less than $5/hr
            
    # Case B: Monthly Rate (Default assumption if not hourly)
    else:
        if max_val < 500:
            return True # FILTER OUT: Less than $500/mo
            
    return False # Salary is OK

def save_excel_locally(data_list, filename):
    try:
        df = pd.DataFrame(data_list)
        with pd.ExcelWriter(filename, engine='xlsxwriter') as writer:
            df.to_excel(writer, sheet_name='Sales Pipeline', index=False)
            workbook = writer.book
            worksheet = writer.sheets['Sales Pipeline']
            
            header_fmt = workbook.add_format({'bold': True, 'bg_color': '#4472C4', 'font_color': 'white', 'border': 1})
            contact_fmt = workbook.add_format({'bg_color': '#FFF2CC', 'border': 1, 'valign': 'top', 'text_wrap': True})
            
            worksheet.freeze_panes(1, 0) 
            for col_num, value in enumerate(df.columns.values):
                worksheet.write(0, col_num, value, header_fmt)
            
            worksheet.set_column('A:A', 30)       
            worksheet.set_column('D:D', 35, contact_fmt)     
            worksheet.set_column('F:F', 50)   

        logging.info(f"💾 Checkpoint: Saved {len(data_list)} leads to {filename}")
    except PermissionError:
        logging.warning(f"⚠️ COULD NOT SAVE: Please close '{filename}'! Will try again next time.")
    except Exception as e:
        logging.error(f"Failed to save local Excel backup: {e}")

def send_email_with_attachment(file_path):
    if not EMAIL_SENDER or not EMAIL_PASSWORD:
        logging.warning("Email credentials missing. Skipping.")
        return
    try:
        logging.info(f"Sending report to {EMAIL_RECIPIENT}...")
        msg = MIMEMultipart()
        msg['From'] = EMAIL_SENDER
        msg['To'] = EMAIL_RECIPIENT
        msg['Subject'] = f"New Sales Leads Report - {datetime.now().strftime('%d/%m/%Y')}"
        msg.attach(MIMEText("Attached is the latest Job Scraper Report.\n\nBest,\nAI Agent", 'plain', 'utf-8'))
        
        with open(file_path, "rb") as attachment:
            part = MIMEBase("application", "octet-stream")
            part.set_payload(attachment.read())
        encoders.encode_base64(part)
        part.add_header("Content-Disposition", f"attachment; filename= {os.path.basename(file_path)}")
        msg.attach(part)

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
    try:
        if not os.path.exists('credentials.json'): return
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name('credentials.json', scope)
        gs_client = gspread.authorize(creds)
        sheet = gs_client.open("OJ_Scraper_Leads").sheet1
        
        rows_to_upload = []
        for entry in data_list:
            rows_to_upload.append([
                entry["Job Title"], entry["Salary"], entry["Post Date"], 
                entry["Contact Info"], entry["Link"], entry["Description"], "New", ""
            ])
        if rows_to_upload:
            sheet.append_rows(rows_to_upload)
            logging.info(f"Successfully uploaded {len(rows_to_upload)} rows to Google Sheets!")
    except Exception as e:
        logging.error(f"Failed to update Google Sheets: {e}")

def extract_contact_with_ai(overview_text):
    if not overview_text or len(overview_text) < 15: return "None"
    
    logging.info("Speed mode: Waiting 3 seconds...")
    time.sleep(3) 

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
    logging.info("STARTING BACKGROUND JOB RUN (HEADLESS MODE)...")
    filename = f"OJ_Sales_Leads_{time.strftime('%Y%m%d_%H%M')}.xlsx"
    
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
             browser_context.close(); return

        logging.info("Navigating to job search results...")
        page.goto("https://www.onlinejobs.ph/jobseekers/jobsearch", wait_until="domcontentloaded", timeout=0)
        
        try:
            page.wait_for_selector('.results', timeout=0)
        except Exception:
             logging.error("Could not load results.")
             browser_context.close(); return
             
        time.sleep(3)

        results = [] 
        seen_ids = set()
        stop_scraping = False 
        
        while not stop_scraping:
            logging.info(f"Scanning page... Leads collected so far: {len(results)}")
            
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

            for profile_url in unique_urls:
                full_url = "https://www.onlinejobs.ph" + profile_url
                if r and r.exists(full_url):
                    logging.warning(f"Skipping cached lead: {full_url}")
                    continue

                try:
                    candidate_page = browser_context.new_page()
                    candidate_page.goto(full_url, wait_until="domcontentloaded", timeout=0)
                    time.sleep(2)

                    # Extract basic data
                    date_sel = "body > div > section.bg-ltblue.pt-4.pt-lg-0 > div > div.card.job-post.shadow.mb-4.mb-md-0 > div > div > div:nth-child(4) > dl > dd > p"
                    post_date_str = candidate_page.locator(date_sel).inner_text().strip() if candidate_page.locator(date_sel).count() > 0 else "N/A"
                    
                    if parse_job_date(post_date_str) < CUTOFF_DATE:
                        logging.warning(f"Reached cutoff date ({post_date_str}). Stopping scraper!")
                        stop_scraping = True; candidate_page.close(); break 

                    title = candidate_page.locator("h1").first.inner_text().strip() if candidate_page.locator("h1").count() > 0 else "N/A"
                    salary = candidate_page.locator("dl > dd > p").nth(0).inner_text().strip() if candidate_page.locator("dl > dd > p").count() > 0 else "N/A"
                    
                    # --- NEW: SALARY FILTER CHECK ---
                    if is_salary_too_low(salary):
                        logging.warning(f"📉 Low Salary Detected: {salary}. Skipping & Marking as Processed.")
                        if r: r.set(full_url, "processed", ex=2592000)
                        candidate_page.close()
                        continue
                    
                    desc = candidate_page.locator('//*[@id="job-description"]').inner_text().strip() if candidate_page.locator('//*[@id="job-description"]').count() > 0 else "N/A"

                    logging.info(f"Processing: {title} | Salary: {salary}")
                    
                    contact_info = extract_contact_with_ai(desc)

                    # --- DOUBLE FILTER VALIDATION ---
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
                            "Job Title": title, "Salary": salary, "Post Date": post_date_str,
                            "Contact Info": contact_info, "Link": full_url, "Description": desc
                        }
                        results.append(new_lead)
                        if r: r.set(full_url, "processed", ex=2592000) 
                        logging.info(f"✅ Saved Lead! Found: {contact_info} (Total: {len(results)})")

                        if len(results) % 5 == 0:
                            save_excel_locally(results, filename)
                    else:
                        logging.warning(f"Discarding (Filtered Output): {contact_info}")

                    candidate_page.close()
                except Exception as e:
                    logging.error(f"Error processing {full_url}: {e}")
                    try: candidate_page.close() 
                    except Exception: pass
                    continue
            
            if stop_scraping: break
            
            # --- PAGINATION ---
            logging.info("Moving to NEXT page...")
            try:
                next_btn = page.locator('ul.pagination li a:has-text(">")') 
                if next_btn.count() > 0 and next_btn.is_visible():
                    next_btn.click(force=True)
                    logging.info("Clicked Next >")
                    try:
                        page.wait_for_load_state("domcontentloaded", timeout=0)
                        time.sleep(3)
                    except:
                        time.sleep(10)
                else:
                    logging.warning("No 'Next' page found (End of results). Stopping.")
                    break
            except Exception as e:
                logging.error(f"Pagination error: {e}")
                break

        browser_context.close()
        
        if results:
            save_excel_locally(results, filename)
            update_google_sheets(results)
            send_email_with_attachment(filename)
            os.system(f"open '{filename}'")
            logging.info("Done.")

if __name__ == "__main__":
    run_job_seeker_agent()