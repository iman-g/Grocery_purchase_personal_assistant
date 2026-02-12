import pandas as pd
import gspread
from rapidfuzz import process, fuzz
import time

# --- CONFIGURATION ---
SHEET_URL = "https://docs.google.com/spreadsheets/d/1tCg-sqNG3HWTDpdDuNIpVoIuImWU5rqMdelr9rxEu8E/edit"
SHEET_TAB_NAME = "Raw"
MEMORY_FILE = "product_translation_memory.csv"
CREDENTIALS_FILE = "grocery_tracker.json"
MATCH_THRESHOLD = 85

def authenticate_google_sheets():
    try:
        gc = gspread.service_account(filename=CREDENTIALS_FILE)
        sh = gc.open_by_url(SHEET_URL)
        return sh.worksheet(SHEET_TAB_NAME)
    except Exception as e:
        print(f"❌ Auth Failed: {e}")
        return None

def load_database():
    try:
        df = pd.read_csv(MEMORY_FILE, dtype={'id': str})
        print(f"🧠 Database Loaded: {len(df)} products.")
        return df
    except FileNotFoundError:
        print(f"❌ Could not find {MEMORY_FILE}")
        return None

def find_best_matches(query, choices_dict, limit=3):
    results = process.extract(
        query, 
        choices_dict.keys(), 
        scorer=fuzz.WRatio, 
        limit=limit
    )
    valid_matches = []
    for match_name, score, _ in results:
        if score >= MATCH_THRESHOLD:
            prod_id = choices_dict[match_name]
            valid_matches.append((prod_id, match_name, score))
    return valid_matches

def run_mapping_pipeline():
    print("🚀 Starting Smart Mapping Pipeline...")
    
    # 1. Setup
    worksheet = authenticate_google_sheets()
    if not worksheet: return
    
    db_df = load_database()
    if db_df is None: return

    # Database Lookup: {Dutch Title: ID}
    db_choices = pd.Series(db_df.id.values, index=db_df.dutch_title).to_dict()
    
    # 2. Get Sheet Data
    print("📥 Fetching Google Sheet data...")
    all_records = worksheet.get_all_records()
    df_sheet = pd.DataFrame(all_records)
    
    # Validation
    if 'id' not in df_sheet.columns:
        print("   Creating columns...")
        worksheet.add_cols(2)
        df_sheet['id'] = ""
        df_sheet['ids'] = ""

    # --- STRATEGY: LEARN FROM SHEET HISTORY ---
    # Build a lookup of product_original -> (id, ids) from rows that are ALREADY mapped
    print("   Building internal history...")
    known_mappings = {}
    
    # Filter for rows that HAVE an ID
    mapped_rows = df_sheet[df_sheet['id'].astype(str).str.strip() != ""]
    
    for _, row in mapped_rows.iterrows():
        p_name = str(row['product_original']).strip()
        p_id = str(row['id']).strip()
        p_ids = str(row['ids']).strip()
        
        # Store if valid
        if p_name and p_id:
            known_mappings[p_name] = (p_id, p_ids)
            
    print(f"   ↳ Learned {len(known_mappings)} exact mappings from history.")

    # 3. Identify Rows to Process
    # Criteria: Store is AH AND id is empty
    to_process_indices = []
    
    for index, row in df_sheet.iterrows():
        store = str(row.get('store', '')).lower()
        existing_id = str(row.get('id', '')).strip()
        
        # NOTE: I assumed you want 'in' AH. If you meant 'not in', change this back!
        if 'albert_heijn' in store and not existing_id:
            to_process_indices.append(index)
            
    print(f"🔍 Found {len(to_process_indices)} unmapped AH purchases.")
    
    if len(to_process_indices) == 0:
        print("✅ No new rows to map.")
        return

    # 4. Processing Loop
    updates = []
    
    # Pre-calculate column indices (0-based in df, 1-based in Sheet)
    col_id_idx = df_sheet.columns.get_loc("id") + 1
    col_ids_idx = df_sheet.columns.get_loc("ids") + 1

    for idx in to_process_indices:
        sheet_row_num = idx + 2 
        product_name = str(df_sheet.at[idx, 'product_original']).strip()
        
        if not product_name: continue

        print(f"   Processing: '{product_name}'...", end="")
        
        # --- CHECK 1: EXACT MATCH IN HISTORY ---
        if product_name in known_mappings:
            # We found it in our own sheet history! Copy the ID.
            found_id, found_ids_str = known_mappings[product_name]
            print(f" ⚡ HISTORY MATCH: {found_id}")
            
            updates.append({
                'range': gspread.utils.rowcol_to_a1(sheet_row_num, col_id_idx),
                'values': [[str(found_id)]]
            })
            updates.append({
                'range': gspread.utils.rowcol_to_a1(sheet_row_num, col_ids_idx),
                'values': [[str(found_ids_str)]]
            })
            continue # Skip to next row

        # --- CHECK 2: FUZZY MATCH DB ---
        # If not in history, ask the database
        matches = find_best_matches(product_name, db_choices)
        
        if matches:
            best_id, best_name, best_score = matches[0]
            ids_str = "; ".join([f"{m[0]} ({int(m[2])}%)" for m in matches])
            
            print(f" 🤖 FUZZY MATCH: {best_id} ({int(best_score)}%)")
            
            updates.append({
                'range': gspread.utils.rowcol_to_a1(sheet_row_num, col_id_idx),
                'values': [[str(best_id)]]
            })
            updates.append({
                'range': gspread.utils.rowcol_to_a1(sheet_row_num, col_ids_idx),
                'values': [[ids_str]]
            })
            
            # OPTIONAL: Add this new find to known_mappings so subsequent rows
            # in THIS batch use it immediately without recalculating fuzzy match
            known_mappings[product_name] = (best_id, ids_str)
            
        else:
            print(f" ⚠️ No match > {MATCH_THRESHOLD}%")
            updates.append({
                'range': gspread.utils.rowcol_to_a1(sheet_row_num, col_ids_idx),
                'values': [['No match found']]
            })

    # 5. Batch Update
    if updates:
        print(f"💾 Saving {len(updates)} changes to Google Sheets...")
        try:
            worksheet.batch_update(updates)
            print("🎉 Success!")
        except Exception as e:
            print(f"❌ Batch Update Failed: {e}")
    else:
        print("No matches found to update.")

if __name__ == "__main__":
    run_mapping_pipeline()