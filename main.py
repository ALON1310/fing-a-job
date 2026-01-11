import os
import sys
import subprocess
import time
from dotenv import load_dotenv

# Load default env vars (just in case)
load_dotenv()

def clear_screen():
    os.system('cls' if os.name == 'nt' else 'clear')

def print_header():
    print("=" * 50)
    print("   🤖  PLATONICS LEAD MACHINE - CONTROL CENTER   ")
    print("=" * 50)

def run_script(script_name):
    """Runs a python script in a subprocess"""
    print(f"\n🚀 Launching {script_name}...\n")
    try:
        # sys.executable ensures we use the same python interpreter (venv)
        subprocess.run([sys.executable, script_name], check=True)
        print(f"\n✅ {script_name} finished successfully.\n")
    except subprocess.CalledProcessError:
        print(f"\n❌ Error: {script_name} crashed.\n")
    except KeyboardInterrupt:
        print("\n🛑 Stopped by user.\n")
    
    input("Press Enter to continue...")

def main():
    while True:
        clear_screen()
        print_header()
        print("\nChoose an Action:")
        print("1. 🎣  Fetch Leads (Scraper Agent)")
        print("2. 🧪  Dry Run (Verify & Sort - No Sending)")
        print("3. 🧹  Maintenance (Clean PENDING & Fix DB)")
        print("4. 📊  Dashboard (Launch UI)")
        print("-" * 30)
        print("5. 🚀  SEND REAL EMAILS (Real Mode)")
        print("6. 🤖  RUN DAILY AUTOMATION (Follow-ups & Archive)")
        print("-" * 30)
        print("q. Quit")
        
        choice = input("\nSelect option: ").strip().lower()

        if choice == '1':
            run_script("scraper_agent.py")

        elif choice == '2':
            os.environ["MODE"] = "DRYRUN"
            os.environ["VERIFY_ONLY"] = "0"
            run_script("sender_agent.py")

        elif choice == '3':
            run_script("maintenance_tool.py")

        elif choice == '4':
            print("\n📊 Opening Dashboard...")
            try:
                subprocess.run(["streamlit", "run", "dashboard.py"])
            except KeyboardInterrupt:
                pass

        elif choice == '5':
            print("\n⚠️  WARNING: YOU ARE ABOUT TO SEND REAL EMAILS! ⚠️")
            confirm = input("Type 'YES' to confirm: ")
            if confirm == "YES":
                os.environ["MODE"] = "REAL"
                os.environ["VERIFY_ONLY"] = "0"
                run_script("sender_agent.py")
            else:
                print("❌ Cancelled.")
                time.sleep(1)
        
        elif choice == '6':
            print("\n🤖 Running Automation Manager...")
            # שים לב: זה ירוץ לפי ההגדרות ב-.env שלך (REAL או DRYRUN)
            run_script("automation_manager.py")

        elif choice == 'q':
            print("Bye! 👋")
            break
        else:
            print("Invalid option!")
            time.sleep(1)

if __name__ == "__main__":
    main()