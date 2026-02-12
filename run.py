import sys
import time
from datetime import datetime

# Import modules
try:
    import lidl
    import albert_heijn
    import file_trans
except ImportError as e:
    print(f"❌ Error: {e}")
    print("Ensure lidl.py, albert_heijn.py, and file_trans.py are in the same folder.")
    sys.exit(1)

def main():
    start_time = time.time()
    today_str = datetime.now().strftime("%Y-%m-%d")
    
    print(f"==================================================")
    print(f"   🚀 DAILY DATA PIPELINE STARTED: {today_str}")
    print(f"==================================================")

    # --- STEP 1: LIDL ---
    print("\n[1/3] Scraping Lidl...")
    try:
        lidl_file = lidl.scrape_lidl_final_refined()
    except Exception as e:
        print(f"❌ Lidl Failed: {e}")
        lidl_file = None

    # --- STEP 2: ALBERT HEIJN ---
    print("\n[2/3] Scraping Albert Heijn...")
    ah_export_file = None
    ah_summary_file = None
    try:
        ah_export_file, ah_summary_file = albert_heijn.scrape_ah_final()
    except Exception as e:
        print(f"❌ AH Failed: {e}")

    # --- STEP 3: TRANSLATE ---
    print("\n[3/3] Translating Data...")
    if any([lidl_file, ah_export_file, ah_summary_file]):
        file_trans.run_translation_pipeline(
            lidl_file=lidl_file,
            ah_export_file=ah_export_file,
            ah_summary_file=ah_summary_file
        )
    else:
        print("⚠️ No files to translate.")

    elapsed = (time.time() - start_time) / 60
    print(f"\n==================================================")
    print(f"✅ PIPELINE COMPLETED in {elapsed:.2f} minutes")
    print(f"==================================================")

if __name__ == "__main__":
    main()