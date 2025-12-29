import os
import re
import pandas as pd
import time
from playwright.sync_api import sync_playwright

def run_job_seeker_agent():
    with sync_playwright() as p:
        user_data_dir = os.path.join(os.getcwd(), "user_data")
        
        browser_context = p.chromium.launch_persistent_context(
            user_data_dir,
            headless=False,
            args=["--disable-blink-features=AutomationControlled"]
        )
        
        page = browser_context.pages[0] if browser_context.pages else browser_context.new_page()

        # --- PHASE 1: AUTHENTICATION ---
        print("\n" + "="*50)
        print("PHASE 1: AUTHENTICATION")
        page.goto("https://www.onlinejobs.ph/login", wait_until="networkidle")
        input("\n>>> Login manually, then press ENTER here to start scraping <<<")
        print("="*50 + "\n")

        # --- PHASE 2: NAVIGATION & STRICT DEDUPLICATION ---
        page.goto("https://www.onlinejobs.ph/jobseekers/jobsearch", wait_until="networkidle")
        page.wait_for_selector('.results', timeout=20000)
        time.sleep(3)

        # Extracting links using numeric ID to avoid duplicates
        raw_links = page.locator('div.desc a[href*="/jobseekers/job/"]').all()
        unique_urls = []
        seen_ids = set()
        
        for link in raw_links:
            url = link.get_attribute("href")
            if url:
                # Extract the numeric ID at the end of the URL
                match = re.search(r'(\d+)$', url.split('?')[0].rstrip('/'))
                if match:
                    job_id = match.group(1)
                    if job_id not in seen_ids:
                        seen_ids.add(job_id)
                        unique_urls.append(url)

        print(f"Found {len(unique_urls)} unique candidates. Starting Precision Extraction...")
        results = [] 
        
        for profile_url in unique_urls:
            try:
                full_url = "https://www.onlinejobs.ph" + profile_url if not profile_url.startswith("http") else profile_url
                candidate_page = browser_context.new_page()
                candidate_page.goto(full_url, wait_until="domcontentloaded", timeout=60000)
                time.sleep(2)

                # --- PRECISION EXTRACTION ---
                # 1. Job Type
                jt_sel = "body > div > section.bg-primary.section-perks.pt-4.position-relative > div > div > div > h1"
                job_type = candidate_page.locator(jt_sel).inner_text().strip() if candidate_page.locator(jt_sel).count() > 0 else "N/A"

                # 2. Employment Type (Investment)
                emp_sel = "body > div > section.bg-ltblue.pt-4.pt-lg-0 > div > div.card.job-post.shadow.mb-4.mb-md-0 > div > div > div:nth-child(1) > dl > dd > p"
                emp_type = candidate_page.locator(emp_sel).inner_text().strip() if candidate_page.locator(emp_sel).count() > 0 else "N/A"

                # 3. Salary
                sal_sel = "body > div > section.bg-ltblue.pt-4.pt-lg-0 > div > div.card.job-post.shadow.mb-4.mb-md-0 > div > div > div:nth-child(2) > dl > dd > p"
                salary = candidate_page.locator(sal_sel).inner_text().strip() if candidate_page.locator(sal_sel).count() > 0 else "N/A"

                # 4. Hours per Week
                hrs_sel = "body > div > section.bg-ltblue.pt-4.pt-lg-0 > div > div.card.job-post.shadow.mb-4.mb-md-0 > div > div > div:nth-child(3) > dl > dd > p"
                hours = candidate_page.locator(hrs_sel).inner_text().strip() if candidate_page.locator(hrs_sel).count() > 0 else "N/A"

                # 5. Last Updated
                upd_sel = "body > div > section.bg-ltblue.pt-4.pt-lg-0 > div > div.card.job-post.shadow.mb-4.mb-md-0 > div > div > div:nth-child(4) > dl > dd > p"
                date_val = candidate_page.locator(upd_sel).inner_text().strip() if candidate_page.locator(upd_sel).count() > 0 else "N/A"

                # 6. Job Overview
                overview_sel = '//*[@id="job-description"]'
                overview = candidate_page.locator(overview_sel).inner_text().strip() if candidate_page.locator(overview_sel).count() > 0 else "N/A"

                # --- EMAIL EXTRACTION ---
                email = "Not Found"
                show_btn = candidate_page.get_by_text("Show Contact Information", exact=False)
                if show_btn.is_visible():
                    try:
                        show_btn.scroll_into_view_if_needed()
                        show_btn.click(force=True)
                        candidate_page.wait_for_timeout(4500) 
                        mail_link = candidate_page.locator('a[href^="mailto:"]').first
                        if mail_link.count() > 0:
                            email = mail_link.inner_text().strip()
                        else:
                            email_box = candidate_page.locator(':text("@")').first
                            if email_box.count() > 0:
                                email = email_box.inner_text().strip()
                    except Exception:
                        email = "Click Failed"

                print(f"[{len(results)+1}] Scraped: {job_type} | Updated: {date_val}")

                results.append({
                    "Job Type": job_type,
                    "Email": email,
                    "Salary": salary,
                    "Employment Type": emp_type,
                    "Hours/Week": hours,
                    "Last Updated": date_val,
                    "Job Overview": overview[:800],
                    "Link": full_url
                })
                
                candidate_page.close() 
                if len(results) >= 6:
                    break 

            except Exception as e:
                print(f"Error on profile: {e}")
                continue

        # --- PHASE 3: EXPORT ---
        if results:
            df = pd.DataFrame(results)
            filename = f"OJ_Final_Precision_Report_{time.strftime('%Y%m%d_%H%M')}.xlsx"
            df.to_excel(filename, index=False)
            print(f"\n--- SUCCESS! Created {filename} ---")
        
        browser_context.close()

if __name__ == "__main__":
    run_job_seeker_agent()