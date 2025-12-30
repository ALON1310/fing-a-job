import os
import re
import pandas as pd
import time
import redis
import logging
import colorlog  # Added: Library for colored logs
from google import genai 
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright

# --- 1. COLORED LOGGING CONFIGURATION ---
# Creating a custom colored formatter
formatter = colorlog.ColoredFormatter(
    "%(log_color)s%(asctime)s - %(levelname)s - %(message)s",
    datefmt='%Y-%m-%d %H:%M:%S',
    log_colors={
        'DEBUG':    'cyan',
        'INFO':     'white',
        'WARNING':  'yellow',
        'ERROR':    'red',
        'CRITICAL': 'red,bg_white',
    }
)

# Stream Handler (for the Terminal with colors)
stream_handler = colorlog.StreamHandler()
stream_handler.setFormatter(formatter)

# File Handler (for the .log file - no colors allowed in text files)
file_handler = logging.FileHandler("scraper.log")
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))

# Applying configuration to the logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)
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
                # Using yellow for skipped items to make them stand out
                logging.warning(f"Skipping cached lead: {full_url}")
                continue

            try:
                candidate_page = browser_context.new_page()
                candidate_page.goto(full_url, wait_until="domcontentloaded", timeout=60000)
                time.sleep(2)

                title_sel = "h1"
                job_title = candidate_page.locator(title_sel).first.inner_text().strip() if candidate_page.locator(title_sel).count() > 0 else "N/A"
                
                date_sel = "body > div > section.bg-ltblue.pt-4.pt-lg-0 > div > div.card.job-post.shadow.mb-4.mb-md-0 > div > div > div:nth-child(4) > dl > dd > p"
                post_date = candidate_page.locator(date_sel).inner_text().strip() if candidate_page.locator(date_sel).count() > 0 else "N/A"

                overview_sel = '//*[@id="job-description"]'
                overview = candidate_page.locator(overview_sel).inner_text().strip() if candidate_page.locator(overview_sel).count() > 0 else "N/A"

                if overview != "N/A":
                    logging.info(f"Successfully accessed overview for: {job_title}")

                logging.info(f"[{len(results)+1}] AI Analyzing: {job_title}")
                contact_info = extract_contact_with_ai(overview)

                results.append({
                    "Job Title": job_title,
                    "Post Date": post_date,
                    "AI Refined Contact": contact_info,
                    "Link": full_url
                })
                
                if r:
                    r.set(full_url, "processed")

                candidate_page.close() 
                
                if len(results) >= 6:
                    break 

            except Exception as e:
                logging.error(f"Error processing {full_url}: {e}")
                continue

        if results:
            df = pd.DataFrame(results)
            filename = f"OJ_Final_AI_Report_{time.strftime('%Y%m%d_%H%M')}.xlsx"
            df.to_excel(filename, index=False)
            logging.info(f"Final Report created: {filename}")
        
        browser_context.close()

if __name__ == "__main__":
    run_job_seeker_agent()