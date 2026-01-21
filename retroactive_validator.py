import os
import time
import logging
from dotenv import load_dotenv

# ייבוא הכלים
from sheets_client import get_sheet_client
from utils import setup_logging
import salary_parser

# -----------------------------
# CONFIGURATION
# -----------------------------
load_dotenv()
SHEET_NAME = os.getenv("SHEET", "Master_Leads_DB")

# הגדרות שכר (לפי העדכון האחרון שלך - $8 לשעה מינימום)
MIN_MONTHLY_USD = 1280.0 
MIN_MONTHLY_PHP = 70000.0
UNKNOWN_POLICY = os.getenv("UNKNOWN_SALARY_POLICY", "keep").strip().lower()

setup_logging()

def run_retroactive_validation():
    logging.info("🕵️ STARTING SMART BATCH VALIDATOR (No API Limits)")
    logging.info("--------------------------------------------------")

    # 1. חיבור לגוגל שיטס והורדת כל המידע בפעם אחת
    try:
        client = get_sheet_client()
        ws = client.open(SHEET_NAME).sheet1
        logging.info("📥 Downloading all data from Google Sheets...")
        all_rows = ws.get_all_values() # מביא הכל ברשימה אחת גדולה
        logging.info(f"📚 Total rows in sheet: {len(all_rows)}")
    except Exception as e:
        logging.error(f"❌ Failed to connect to sheets: {e}")
        return

    if len(all_rows) < 2:
        logging.info("⚠️ Sheet is empty or has only headers.")
        return

    headers = all_rows[0]
    data = all_rows[1:] # כל השורות בלי הכותרת

    # מיפוי עמודת השכר
    try:
        col_salary_idx = headers.index("Salary")
    except ValueError:
        logging.error("❌ Column 'Salary' missing in header.")
        return

    rows_to_delete = []

    # 2. מעבר על המידע בזיכרון (מהיר מאוד)
    logging.info("🔍 Analyzing data locally...")
    
    for i, row in enumerate(data):
        # המספר האמיתי של השורה בשיטס הוא:
        # האינדקס (מתחיל ב-0) + 1 (בגלל הכותרת) + 1 (כי שיטס מתחיל ב-1) = i + 2
        real_row_num = i + 2
        
        # הגנה מפני שורות ריקות
        if len(row) <= col_salary_idx:
            continue

        salary_text = row[col_salary_idx]

        # שימוש ב"מוח" של salary_parser
        is_bad = salary_parser.is_salary_too_low(
            salary_text, 
            MIN_MONTHLY_USD, 
            MIN_MONTHLY_PHP, 
            UNKNOWN_POLICY
        )

        if is_bad:
            # אנחנו רק רושמים בצד את המספר, לא מוחקים עדיין
            rows_to_delete.append(real_row_num)
            logging.info(f"   ❌ Row {real_row_num} Marked for deletion: '{salary_text}'")
        else:
            # אופציונלי: להדפיס רק פעם ב-10 שורות כדי לא להציף את הלוג
            pass 

    # 3. ביצוע המחיקה (מהסוף להתחלה!)
    # חייבים למחוק מלמטה למעלה, אחרת המספרים של השורות משתנים תוך כדי תנועה
    if not rows_to_delete:
        logging.info("✅ No bad rows found. Everything looks good!")
        return

    logging.info(f"🗑️ Found {len(rows_to_delete)} rows to delete. Starting cleanup...")
    
    rows_to_delete.sort(reverse=True) # מיון בסדר יורד: 100, 99, 50...

    deleted_count = 0
    for row_num in rows_to_delete:
        try:
            ws.delete_rows(row_num)
            logging.info(f"   🗑️ Deleted Row {row_num}")
            deleted_count += 1
            time.sleep(0.8) # הפסקה קטנה כדי שגוגל לא יחסום אותנו שוב
        except Exception as e:
            logging.error(f"   ⚠️ Error deleting row {row_num}: {e}")

    logging.info(f"🏁 Done. Total rows deleted: {deleted_count}")

if __name__ == "__main__":
    run_retroactive_validation()