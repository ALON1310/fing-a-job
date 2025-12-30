import os
import re
import pandas as pd
import time
from google import genai  # Use only the new 2025 library
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright


# --- 1. SETUP AI & SECURITY ---
load_dotenv()
API_KEY = os.getenv("GEMINI_API_KEY")

if not API_KEY:
    print("ERROR: GEMINI_API_KEY not found in .env file!")
    exit()

# Initialize the Client. 
# Removing version overrides to let the SDK choose the best stable route.
client = genai.Client(api_key=API_KEY)

# Log to check if AI client is configured
print("AI Client configured successfully.")

def extract_contact_with_ai(overview_text):
    """Refines and extracts contact info using the stable SDK path."""
    if not overview_text or overview_text == "N/A" or len(overview_text) < 15:
        return "None"
    
    prompt = f"""
    Extract only contact details (Email, Phone, WhatsApp, LinkedIn) from this text.
    Format: Type: Value.
    If none found, reply ONLY: None.
    
    Text: {overview_text}
    """

    try:
        # Standard stable model identifier
        response = client.models.generate_content(
            model="gemini-2.5-flash",  # Updated to the correct model name
            contents=prompt
        )
        if response and response.text:
            return response.text.strip()
        return "None"
    except Exception as e:
        # If 404 persists, print the full details to help debug
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

        # PHASE 1: LOGIN
        print("\nPHASE 1: AUTHENTICATION")
        page.goto("https://www.onlinejobs.ph/login")
        input(">>> Login manually, then press ENTER here to start scraping <<<")

        # PHASE 2: SEARCH
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
            try:
                full_url = "https://www.onlinejobs.ph" + profile_url
                candidate_page = browser_context.new_page()
                candidate_page.goto(full_url, wait_until="domcontentloaded", timeout=60000)
                time.sleep(2)

                # EXTRACTION
                title_sel = "h1"
                job_title = candidate_page.locator(title_sel).first.inner_text().strip() if candidate_page.locator(title_sel).count() > 0 else "N/A"

# New extraction for Date
                date_sel = "body > div > section.bg-ltblue.pt-4.pt-lg-0 > div > div.card.job-post.shadow.mb-4.mb-md-0 > div > div > div:nth-child(4) > dl > dd > p"
                post_date = candidate_page.locator(date_sel).inner_text().strip() if candidate_page.locator(date_sel).count() > 0 else "N/A"
                overview_sel = '//*[@id="job-description"]'
                overview = candidate_page.locator(overview_sel).inner_text().strip() if candidate_page.locator(overview_sel).count() > 0 else "N/A" 
                # Log to check if Job Overview was successfully accessed
                if overview != "N/A":
                    print(f"Job Overview accessed successfully: {overview[:100]}...")  # Print first 100 characters
                else:
                    print("Failed to access Job Overview.")

                # AI PROCESS
                print(f"[{len(results)+1}] AI Analyzing: {job_title}")
                contact_info = extract_contact_with_ai(overview)

                results.append({
                    "Job Title": job_title,
                    "AI Refined Contact": contact_info,
                    "Link": full_url
                })
                
                candidate_page.close() 
                if len(results) >= 6:
                    break 

            except Exception as e:
                print(f"Error: {e}")
                continue

        if results:
            df = pd.DataFrame(results)
            filename = f"OJ_Final_AI_Report_{time.strftime('%Y%m%d_%H%M')}.xlsx"
            df.to_excel(filename, index=False)
            print(f"\n--- SUCCESS! Final Report created: {filename} ---")
        
        browser_context.close()

if __name__ == "__main__":
    run_job_seeker_agent()