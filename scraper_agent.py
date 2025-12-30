import os
import re
import pandas as pd
import time
import redis  # Library for memory management
from google import genai 
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright

# --- 1. SETUP AI & SECURITY ---
load_dotenv()
API_KEY = os.getenv("GEMINI_API_KEY")

if not API_KEY:
    print("ERROR: GEMINI_API_KEY not found in .env file!")
    exit()

# Initialize the AI Client
client = genai.Client(api_key=API_KEY)
print("AI Client configured successfully.")

# --- 2. REDIS SETUP ---
# Establishing connection to local Redis server
try:
    # decode_responses=True ensures we get strings back from Redis instead of bytes
    r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
    # Test connection
    r.ping()
    print("Redis connection successful.")
except Exception as e:
    print(f"Could not connect to Redis: {e}. Running without cache.")
    r = None

def extract_contact_with_ai(overview_text):
    """Refines and extracts contact info using the specified model."""
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
        print(f"--- AI DEBUG LOG: {e} ---")
        return "None"

def run_job_seeker_agent():
    with sync_playwright() as p:
        user_data_dir = os.path.join(os.getcwd(), "user_data")
        browser_context = p.chromium.launch_persistent_context(
            user_data_dir, headless=False,
            args=["--disable-blink-features=AutomationControlled"]
        )
        
        page = browser_context.pages[0] if browser_context.pages else browser_context.new_page()

        # PHASE 1: AUTHENTICATION
        print("\nPHASE 1: AUTHENTICATION")
        page.goto("https://www.onlinejobs.ph/login")
        input(">>> Login manually, then press ENTER here to start scraping <<<")

        # PHASE 2: SEARCH
        print("Navigating to job search results...")
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
        print(f"Analyzing {len(unique_urls)} leads...")

        for profile_url in unique_urls:
            full_url = "https://www.onlinejobs.ph" + profile_url

            # REDIS CHECK: Skip this lead if the URL is already in Redis
            if r and r.exists(full_url):
                print(f"Skipping already processed lead: {full_url}")
                continue

            try:
                candidate_page = browser_context.new_page()
                candidate_page.goto(full_url, wait_until="domcontentloaded", timeout=60000)
                time.sleep(2)

                # --- EXTRACTION ---
                
                # Job Title
                title_sel = "h1"
                job_title = candidate_page.locator(title_sel).first.inner_text().strip() if candidate_page.locator(title_sel).count() > 0 else "N/A"
                
                # Post Date (Requested Addition)
                date_sel = "body > div > section.bg-ltblue.pt-4.pt-lg-0 > div > div.card.job-post.shadow.mb-4.mb-md-0 > div > div > div:nth-child(4) > dl > dd > p"
                post_date = candidate_page.locator(date_sel).inner_text().strip() if candidate_page.locator(date_sel).count() > 0 else "N/A"

                # Job Overview
                overview_sel = '//*[@id="job-description"]'
                overview = candidate_page.locator(overview_sel).inner_text().strip() if candidate_page.locator(overview_sel).count() > 0 else "N/A"

                if overview != "N/A":
                    print(f"Job Overview accessed successfully for: {job_title}")

                # --- AI PROCESS ---
                print(f"[{len(results)+1}] AI Analyzing: {job_title}")
                contact_info = extract_contact_with_ai(overview)

                # Store data in results list
                results.append({
                    "Job Title": job_title,
                    "Post Date": post_date,
                    "AI Refined Contact": contact_info,
                    "Link": full_url
                })
                
                # REDIS SAVE: Mark this URL as processed
                if r:
                    r.set(full_url, "processed")

                candidate_page.close() 
                
                # Limit results to 6 as per original logic
                if len(results) >= 6:
                    break 

            except Exception as e:
                print(f"Error processing {full_url}: {e}")
                continue

        # --- PHASE 3: EXPORT ---
        if results:
            df = pd.DataFrame(results)
            filename = f"OJ_Final_AI_Report_{time.strftime('%Y%m%d_%H%M')}.xlsx"
            df.to_excel(filename, index=False)
            print(f"\n--- SUCCESS! Final Report created: {filename} ---")
        
        browser_context.close()

if __name__ == "__main__":
    run_job_seeker_agent()