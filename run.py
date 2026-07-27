# import sys
# import time
# from datetime import datetime

# # Import modules
# try:
#     import lidl
#     import albert_heijn
#     import file_trans
#     import map_purchases
# except ImportError as e:
#     print(f"❌ Error: {e}")
#     print("Ensure lidl.py, albert_heijn.py, file_trans.py, and map_purchases.py are in the same folder.")
#     sys.exit(1)

# def main():
#     start_time = time.time()
#     today_str = datetime.now().strftime("%Y-%m-%d")
    
#     print(f"==================================================")
#     print(f"   🚀 DAILY DATA PIPELINE STARTED: {today_str}")
#     print(f"==================================================")

#     # --- STEP 1: LIDL SCRAPING ---
#     print("\n[1/4] Scraping Lidl...")
#     lidl_file = None
#     try:
#         lidl_file = lidl.scrape_lidl_final_refined()
#     except Exception as e:
#         print(f"❌ Lidl Failed: {e}")

#     # --- STEP 2: ALBERT HEIJN SCRAPING ---
#     print("\n[2/4] Scraping Albert Heijn...")
#     ah_export_file = None
#     ah_summary_file = None
#     try:
#         ah_export_file, ah_summary_file = albert_heijn.scrape_ah_final()
#     except Exception as e:
#         print(f"❌ AH Failed: {e}")

#     # --- STEP 3: TRANSLATION ---
#     print("\n[3/4] Translating Data...")
#     if any([lidl_file, ah_export_file, ah_summary_file]):
#         try:
#             file_trans.run_translation_pipeline(
#                 lidl_file=lidl_file,
#                 ah_export_file=ah_export_file,
#                 ah_summary_file=ah_summary_file
#             )
#         except Exception as e:
#             print(f"❌ Translation Failed: {e}")
#     else:
#         print("⚠️ No files to translate.")

#     # --- STEP 4: MAPPING PURCHASES (GOOGLE SHEETS) ---
#     print("\n[4/4] Mapping Purchases to Google Sheets...")
#     try:

#         map_purchases.run_mapping_pipeline()
#     except Exception as e:
#         print(f"❌ Mapping Failed: {e}")
#         print("   (Check your internet or 'grocery_tracker.json' credentials)")

#     # --- FINISH ---
#     elapsed = (time.time() - start_time) / 60
#     print(f"\n==================================================")
#     print(f"✅ PIPELINE COMPLETED in {elapsed:.2f} minutes")
#     print(f"==================================================")

# if __name__ == "__main__":
#     main()



import os
import sys
import time
from datetime import datetime

# Import modules
try:
    import lidl
    import albert_heijn
    import file_trans
    import map_purchases
except ImportError as e:
    print(f"❌ Error: {e}")
    print("Ensure lidl.py, albert_heijn.py, file_trans.py, and map_purchases.py are in the same folder.")
    sys.exit(1)

def main():
    start_time = time.time()
    today_str = datetime.now().strftime("%Y-%m-%d")
    
    print(f"==================================================")
    print(f"   🚀 DAILY DATA PIPELINE STARTED: {today_str}")
    print(f"==================================================")

    # --- STEP 1: LIDL SCRAPING ---
    print("\n[1/4] Scraping Lidl...")
    lidl_file = None
    try:
        lidl_file = lidl.scrape_lidl_final_refined()
    except Exception as e:
        print(f"❌ Lidl Failed: {e}")

    # --- STEP 2: ALBERT HEIJN SCRAPING ---
    print("\n[2/4] Scraping Albert Heijn...")
    ah_export_file = None
    ah_summary_file = None
    # Full, unbounded catalog scrape every run (~31k products) for real daily coverage.
    # The scrape itself is fast (~15-30 min); the slow part historically was translating
    # products never seen before, which file_trans.py now caps (see AH_MAX_NEW_TRANSLATIONS).
    try:
        ah_export_file, ah_summary_file = albert_heijn.scrape_ah_final()
    except Exception as e:
        print(f"❌ AH Failed: {e}")

    # --- STEP 3: TRANSLATION ---
    print("\n[3/4] Translating Data...")
    if lidl_file or ah_export_file or ah_summary_file:
        try:
            file_trans.run_translation_pipeline(
                lidl_file=lidl_file,
                ah_export_file=ah_export_file,
                ah_summary_file=ah_summary_file
            )
        except Exception as e:
            print(f"❌ Translation Failed: {e}")
    else:
        print("⚠️ No files successfully scraped. Skipping translation.")

    # --- STEP 4: MAPPING PURCHASES ---
    print("\n[4/4] Mapping Purchases to Google Sheets...")
    try:
        # Note: map_purchases needs to look for the ORIGINAL filenames now, 
        # since file_trans overwrites them instead of adding '_translated'
        map_purchases.run_mapping_pipeline()
    except Exception as e:
        print(f"❌ Mapping Failed: {e}")
        print("   (Check your internet or 'grocery_tracker.json' credentials)")

    # --- FINISH ---
    elapsed = (time.time() - start_time) / 60
    print(f"\n==================================================")
    print(f"✅ PIPELINE COMPLETED in {elapsed:.2f} minutes")
    print(f"==================================================")

if __name__ == "__main__":
    main()