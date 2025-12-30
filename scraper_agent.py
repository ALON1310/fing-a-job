import os
import re
import pandas as pd
import time
import redis
import logging
import colorlog
from google import genai 
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright

# --- 1. COLORED LOGGING CONFIGURATION ---
formatter = colorlog.ColoredFormatter(
    "%(log_color)s%(asctime)s - %(levelname)s - %(message)s",
    datefmt='%Y-%m-%d %H:%M:%S',
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

# --- 2. SETUP AI & SECURITY ---
load_dotenv()
API_KEY = os.getenv("GEMINI_API_KEY")

if not API_KEY:
    logging.error("GEMINI_API_KEY not found in .env file!")
    exit()

client = genai.Client(api_key=API_KEY)
logging.info("AI Client configured successfully.")

# --- 3. REDIS SETUP ---
try:
    r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
    r.ping()
    logging.info("Redis connection successful.")
except Exception as e:
    logging.error(f"Could not connect to Redis: {e}. Running without cache.")
    r = None

def extract_contact_with_ai(overview_text):
    if not overview_text or overview_text == "N/A" or len(overview_text) < 15:
        return "None"
    
    # --- SMART SLEEP TO AVOID 429 ERROR ---
    # Since your log shows a limit of 5 requests/min, we wait 12-15 seconds
    logging.info("Waiting 12 seconds to respect Google API Rate Limits...")
    time.sleep(12) 

    prompt = f"""
    Extract only contact details (Email, Phone, WhatsApp, LinkedIn) from this text.
    Format: Type: Value.
    If none found, reply ONLY: None.
    
    Text: {overview_text}
    """

    try:
        response = client.models.generate_content(
            model="gemini-2.5-flash", 
            contents=prompt
        )
        if response and response.text:
            return response.text.strip()
        return "None"
    except Exception as e:
        # If we still hit a rate limit, the error will be caught here
        if "429" in str(e):
            logging.error("Rate limit hit again! AI will return 'None' for this lead.")
        else:
            logging.error(f"AI API Error: {e}")
        return "None"

def run_job_seeker_agent():
    with sync_playwright() as p:
        user_data_dir = os.path.join(os.getcwd(), "user_data")
        browser_context = p.chromium.launch_persistent_context(
            user_data_dir, headless=False,
            args=["--disable-blink-features=AutomationControlled"]
        )
        
        page = browser_context.pages[0] if browser_context.pages else browser_context.new_page()

        logging.info("PHASE 1: AUTHENTICATION")
        page.goto("https://www.onlinejobs.ph/login")
        input(">>> Login manually, then press ENTER here to start scraping <<<")

        logging.info("Navigating to job search results...")
        page.goto("https://www.onlinejobs.ph/jobseekers/jobsearch")
        page.wait_for_selector('.results', timeout=20000)
        time.sleep(3)

        raw_links = page.locator('div.desc a[href*="/jobseekers/job/"]').all()
        unique_urls = []
        seen_ids = set()
        
        for link in raw_links:
            url = link.get_attribute("href")
            if url:
                match = re.search(r'(\d+)$', url.split('?')[0].rstrip('/'))
                if match and match.group(1) not in seen_ids:
                    seen_ids.add(match.group(1))
                    unique_urls.append(url)

        results = [] 
        logging.info(f"Analyzing {len(unique_urls)} leads...")

        for profile_url in unique_urls:
            full_url = "https://www.onlinejobs.ph" + profile_url

            if r and r.exists(full_url):
                logging.warning(f"Skipping cached lead: {full_url}")
                continue

            try:
                candidate_page = browser_context.new_page()
                candidate_page.goto(full_url, wait_until="domcontentloaded", timeout=60000)
                time.sleep(2)

                # --- DATA EXTRACTION ---
                
                # Job Title
                title_sel = "h1"
                job_title = candidate_page.locator(title_sel).first.inner_text().strip() if candidate_page.locator(title_sel).count() > 0 else "N/A"
                
                # Salary (New Addition)
                sal_sel = "body > div > section.bg-ltblue.pt-4.pt-lg-0 > div > div.card.job-post.shadow.mb-4.mb-md-0 > div > div > div:nth-child(2) > dl > dd > p"
                salary = candidate_page.locator(sal_sel).inner_text().strip() if candidate_page.locator(sal_sel).count() > 0 else "N/A"

                # Post Date
                date_sel = "body > div > section.bg-ltblue.pt-4.pt-lg-0 > div > div.card.job-post.shadow.mb-4.mb-md-0 > div > div > div:nth-child(4) > dl > dd > p"
                post_date = candidate_page.locator(date_sel).inner_text().strip() if candidate_page.locator(date_sel).count() > 0 else "N/A"

                # Job Overview
                overview_sel = '//*[@id="job-description"]'
                overview = candidate_page.locator(overview_sel).inner_text().strip() if candidate_page.locator(overview_sel).count() > 0 else "N/A"

                if overview != "N/A":
                    logging.info(f"Processing: {job_title} | Salary: {salary}")

                # --- AI PROCESS ---
                logging.info(f"[{len(results)+1}] AI Analyzing contact info...")
                contact_info = extract_contact_with_ai(overview)

                # ONLY save to Redis if AI process didn't fail completely
                # This way, if you hit a rate limit, you can run again and it will retry the missed ones
                if contact_info != "None":
                    results.append({
                        "Job Title": job_title,
                        "Salary": salary,
                        "Post Date": post_date,
                        "AI Refined Contact": contact_info,
                        "Link": full_url
                    })
                    if r:
                        r.set(full_url, "processed")
                else:
                    logging.warning(f"AI failed to extract info for {job_title}. Will not cache, so we can retry later.")
                
                if r:
                    r.set(full_url, "processed")

                candidate_page.close() 
                
                if len(results) >= 10: # Updated to 10 for better sample
                    break 

            except Exception as e:
                logging.error(f"Error processing {full_url}: {e}")
                continue

        if results:
            df = pd.DataFrame(results)
            filename = f"OJ_AI_Scraper_Report_{time.strftime('%Y%m%d_%H%M')}.xlsx"
            df.to_excel(filename, index=False)
            logging.info(f"Final Report created successfully: {filename}")
        
        browser_context.close()

if __name__ == "__main__":
    run_job_seeker_agent()