import os
import pandas as pd
import time
from playwright.sync_api import sync_playwright

def run_job_seeker_agent():
    with sync_playwright() as p:
        # Directory for browser persistence
        user_data_dir = os.path.join(os.getcwd(), "user_data")
        
        browser_context = p.chromium.launch_persistent_context(
            user_data_dir,
            headless=False,
            args=["--disable-blink-features=AutomationControlled"]
        )
        
        page = browser_context.pages[0] if browser_context.pages else browser_context.new_page()

        # --- PHASE 1: MANUAL LOGIN ---
        print("\n" + "="*50)
        print("PHASE 1: AUTHENTICATION")
        page.goto("https://www.onlinejobs.ph/login", wait_until="networkidle")
        
        print("\n>>> ACTION REQUIRED: <<<")
        print("1. Log in manually in the browser window.")
        print("2. Once logged in and viewing search results, return here.")
        input("--- PRESS ENTER HERE TO START DEEP SCRAPING ---")
        print("="*50 + "\n")

        # --- PHASE 2: SEARCH NAVIGATION ---
        print("Navigating to search results...")
        page.goto("https://www.onlinejobs.ph/jobseekers/jobsearch", wait_until="networkidle")
        page.wait_for_selector('.results', timeout=20000)
        time.sleep(3)

        print("Starting Precision Extraction...")
        
        job_links = page.locator('div.desc a[href*="/jobseekers/job/"]').all()
        seen_links = set()
        results = [] 
        
        for link in job_links:
            profile_url = link.get_attribute("href")
            if not profile_url or profile_url in seen_links or "target=" in profile_url:
                continue
            seen_links.add(profile_url)
            
            try:
                full_url = "https://www.onlinejobs.ph" + profile_url if not profile_url.startswith("http") else profile_url
                
                # Open a new tab for deep scraping
                candidate_page = browser_context.new_page()
                candidate_page.goto(full_url, wait_until="domcontentloaded", timeout=60000)
                time.sleep(2)

                # --- PRECISION EXTRACTION USING YOUR PROVIDED SELECTORS ---
                
                # 1. Job Type
                job_type = "N/A"
                sel_job_type = "body > div > section.bg-primary.section-perks.pt-4.position-relative > div > div > div > h1"
                if candidate_page.locator(sel_job_type).count() > 0:
                    job_type = candidate_page.locator(sel_job_type).inner_text().strip()

                # 2. Employment Type / Investment (Full-time/Part-time)
                employment_type = "N/A"
                sel_emp = "body > div > section.bg-ltblue.pt-4.pt-lg-0 > div > div.card.job-post.shadow.mb-4.mb-md-0 > div > div > div:nth-child(1) > dl > dd > p"
                if candidate_page.locator(sel_emp).count() > 0:
                    employment_type = candidate_page.locator(sel_emp).inner_text().strip()

                # 3. Salary
                salary = "N/A"
                sel_salary = "body > div > section.bg-ltblue.pt-4.pt-lg-0 > div > div.card.job-post.shadow.mb-4.mb-md-0 > div > div > div:nth-child(2) > dl > dd > p"
                if candidate_page.locator(sel_salary).count() > 0:
                    salary = candidate_page.locator(sel_salary).inner_text().strip()

                # 4. Hours per Week
                hours_per_week = "N/A"
                sel_hours = "body > div > section.bg-ltblue.pt-4.pt-lg-0 > div > div.card.job-post.shadow.mb-4.mb-md-0 > div > div > div:nth-child(3) > dl > dd > p"
                if candidate_page.locator(sel_hours).count() > 0:
                    hours_per_week = candidate_page.locator(sel_hours).inner_text().strip()

                # 5. Last Updated
                last_updated = "N/A"
                sel_updated = "body > div > section.bg-ltblue.pt-4.pt-lg-0 > div > div.card.job-post.shadow.mb-4.mb-md-0 > div > div > div:nth-child(4) > dl > dd > p"
                if candidate_page.locator(sel_updated).count() > 0:
                    last_updated = candidate_page.locator(sel_updated).inner_text().strip()

                # 6. Job Overview (Using the ID you provided)
                job_overview = "N/A"
                if candidate_page.locator('//*[@id="job-description"]').count() > 0:
                    job_overview = candidate_page.locator('//*[@id="job-description"]').inner_text().strip()

                # --- ATTEMPT EMAIL EXTRACTION ---
                email = "Check Permissions"
                show_btn = candidate_page.get_by_text("Show Contact Information", exact=False)
                if show_btn.is_visible():
                    try:
                        show_btn.click()
                        candidate_page.wait_for_timeout(1500)
                        mail_link = candidate_page.locator('a[href^="mailto:"]').first
                        if mail_link.count() > 0:
                            email = mail_link.inner_text().strip()
                    except:
                        pass

                print(f"[{len(results)+1}] Scraped: {job_type} | {salary}")

                results.append({
                    "Job Type": job_type,
                    "Salary": salary,
                    "Email": email,
                    "Employment Type": employment_type,
                    "Hours/Week": hours_per_week,
                    "Last Updated": last_updated,
                    "Job Overview": job_overview[:200] + "...", # Limiting text for Excel cell size
                    "Link": full_url
                })
                
                candidate_page.close() 
                
                # Scraping limit
                if len(results) >= 30: 
                    break 

            except Exception as e:
                print(f"Error scraping profile: {e}")
                continue

        # --- PHASE 3: EXPORT ---
        if results:
            df = pd.DataFrame(results)
            filename = f"OJ_Precision_Report_{time.strftime('%Y%m%d_%H%M')}.xlsx"
            df.to_excel(filename, index=False)
            print(f"\n--- SUCCESS! Created {filename} ---")
        
        browser_context.close()

if __name__ == "__main__":
    run_job_seeker_agent()