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
        return False  # Keep if unknown
    
    # Clean string
    clean_text = salary_str.lower().replace(",", "")
    
    # Extract all numbers
    numbers = re.findall(r'\d+(?:\.\d+)?', clean_text)
    if not numbers:
        return False  # Keep if no numbers found
    
    # Take the MAXIMUM number found (optimistic check)
    values = [float(n) for n in numbers]
    max_val = max(values)
    
    # Currency Conversion (Approx 58 PHP = 1 USD)
    if "php" in clean_text or "₱" in clean_text:
        max_val = max_val / 58
        
    # Check Thresholds
    if "hour" in clean_text or "hr" in clean_text:
        if max_val < 5:
            return True  # FILTER OUT: Less than $5/hr
    else:
        if max_val < 500:
            return True  # FILTER OUT: Less than $500/mo
            
    return False  # Salary is OK


def save_excel_locally(new_data_list, filename="Master_Leads_DB.xlsx"):
    """
    Saves data to a SINGLE Master Excel file.
    Appends new leads ONLY if they don't already exist (deduplication based on Link).
    Preserves existing Status/Sales Rep changes.
    """
    try:
        new_df = pd.DataFrame(new_data_list)
        
        # 1. Try to load existing file
        if os.path.exists(filename):
            try:
                existing_df = pd.read_excel(filename, sheet_name='Sales Pipeline')
                
                # Convert links to string to ensure accurate comparison
                existing_df['Link'] = existing_df['Link'].astype(str)
                new_df['Link'] = new_df['Link'].astype(str)
                
                # Filter: Keep only new leads that are NOT in the existing file
                # This logic uses the 'Link' column as the unique ID for deduplication
                new_leads_only = new_df[~new_df['Link'].isin(existing_df['Link'])]
                
                if new_leads_only.empty:
                    logging.info("ℹ️ No new unique leads to append.")
                    return # Nothing to save
                
                logging.info(f"🔄 Merging {len(new_leads_only)} new leads into existing {len(existing_df)} records...")
                final_df = pd.concat([existing_df, new_leads_only], ignore_index=True)
                
            except Exception as e:
                logging.warning(f"Could not read existing file (might be corrupt or different format). Overwriting. Error: {e}")
                final_df = new_df
        else:
            # Create new file if it doesn't exist
            logging.info("🆕 Creating new Master DB file...")
            final_df = new_df

        # 2. Ensure Column Order
        cols = ["Job Title", "Salary", "Post Date", "Contact Info", "Link", "Description", "Status", "Sales Rep", "Notes"]
        for col in cols:
            if col not in final_df.columns:
                final_df[col] = ""
        final_df = final_df[cols] 

        # 3. Save with Formatting (XlsxWriter)
        with pd.ExcelWriter(filename, engine='xlsxwriter') as writer:
            final_df.to_excel(writer, sheet_name='Sales Pipeline', index=False)
            workbook = writer.book
            worksheet = writer.sheets['Sales Pipeline']
            
            # --- SETTINGS SHEET ---
            settings_sheet = workbook.add_worksheet('Settings')
            settings_sheet.write('A1', 'Sales Reps')
            settings_sheet.write('B1', 'Statuses')
            default_reps = ['Dor', 'Alon', 'Unassigned']
            default_statuses = ['New', 'In Progress', 'Hot Lead', 'Closed', 'Not Relevant']
            settings_sheet.write_column('A2', default_reps)
            settings_sheet.write_column('B2', default_statuses)
            
            # --- FORMATS ---
            header_fmt = workbook.add_format({'bold': True, 'bg_color': '#4472C4', 'font_color': 'white', 'border': 1, 'align': 'center', 'valign': 'vcenter', 'font_size': 12})
            text_fmt = workbook.add_format({'valign': 'top', 'font_size': 12})
            contact_fmt = workbook.add_format({'bg_color': '#FFF2CC', 'border': 1, 'valign': 'top', 'text_wrap': True, 'font_size': 12})
            desc_fmt = workbook.add_format({'valign': 'top', 'text_wrap': False, 'font_size': 11})
            
            # Trash format (Gray out) - Safely try to add strikethrough
            trash_fmt = workbook.add_format({'font_color': '#999999', 'bg_color': '#F2F2F2'})
            try: trash_fmt.set_font_strike()
            except AttributeError: pass

            # --- APPLY COLUMN SETTINGS ---
            worksheet.freeze_panes(1, 0) 
            worksheet.autofilter(0, 0, len(final_df), len(final_df.columns) - 1)

            worksheet.set_column('A:A', 30, text_fmt)    
            worksheet.set_column('B:C', 15, text_fmt)    
            worksheet.set_column('D:D', 40, contact_fmt) 
            worksheet.set_column('E:E', 20, text_fmt)    
            worksheet.set_column('F:F', 50, desc_fmt)    
            worksheet.set_column('G:G', 15, text_fmt)    
            worksheet.set_column('H:H', 15, text_fmt)    
            worksheet.set_column('I:I', 30, text_fmt)    

            for col_num, value in enumerate(final_df.columns.values):
                worksheet.write(0, col_num, value, header_fmt)

            # --- DROPDOWNS & CONDITIONAL FORMATTING ---
            worksheet.data_validation(1, 6, 5000, 6, {'validate': 'list', 'source': '=Settings!$B$2:$B$20'})
            worksheet.data_validation(1, 7, 5000, 7, {'validate': 'list', 'source': '=Settings!$A$2:$A$100'})
            worksheet.conditional_format(1, 0, 5000, 8, {'type': 'formula', 'criteria': '=$G2="Not Relevant"', 'format': trash_fmt})

        logging.info(f"💾 Database Updated: {filename} now has {len(final_df)} leads.")

    except PermissionError:
        logging.warning(f"⚠️ ERROR: '{filename}' is open! Close it so I can save updates.")
    except Exception as e:
        logging.error(f"Failed to update Master DB: {e}")

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
        if not os.path.exists('credentials.json'):
            logging.warning("Google Sheets credentials missing.")
            return
            
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name('credentials.json', scope)
        gs_client = gspread.authorize(creds)
        sheet = gs_client.open("OJ_Scraper_Leads").sheet1
        
        rows_to_upload = []
        for entry in data_list:
            rows_to_upload.append([
                entry.get("Job Title"), entry.get("Salary"), entry.get("Post Date"), 
                entry.get("Contact Info"), entry.get("Link"), entry.get("Description"), 
                entry.get("Status", "New"), entry.get("Sales Rep", ""), entry.get("Notes", "")
            ])
            
        if rows_to_upload:
            sheet.append_rows(rows_to_upload)
            logging.info(f"Successfully uploaded {len(rows_to_upload)} rows to Google Sheets!")

    except Exception as e:
        logging.error(f"Failed to update Google Sheets: {e}")

def extract_contact_with_ai(overview_text):
    if not overview_text or len(overview_text) < 15:
        return "None"
    
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
                    if candidate_page.locator(date_sel).count() > 0:
                        post_date_str = candidate_page.locator(date_sel).inner_text().strip()
                    else:
                        post_date_str = "N/A"
                    
                    if parse_job_date(post_date_str) < CUTOFF_DATE:
                        logging.warning(f"Reached cutoff date ({post_date_str}). Stopping scraper!")
                        stop_scraping = True
                        candidate_page.close()
                        break 

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
                        logging.warning(f"📉 Low Salary Detected: {salary}. Skipping & Marking as Processed.")
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
                        results.append(new_lead)
                        if r:
                            r.set(full_url, "processed", ex=2592000)
                        logging.info(f"✅ Saved Lead! Found: {contact_info} (Total: {len(results)})")

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
            
            # PAGINATION
            logging.info("Moving to NEXT page...")
            try:
                next_btn = page.locator('ul.pagination li a:has-text(">")') 
                if next_btn.count() > 0 and next_btn.is_visible():
                    next_btn.click(force=True)
                    logging.info("Clicked Next >")
                    try:
                        page.wait_for_load_state("domcontentloaded", timeout=0)
                        time.sleep(3)
                    except Exception:
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