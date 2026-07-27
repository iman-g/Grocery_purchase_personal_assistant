import pandas as pd
from deep_translator import GoogleTranslator
import time
import os

# --- CONFIGURATION ---
BATCH_SIZE = 50
MEMORY_FILE = "product_translation_memory.csv"
# Safety cap: translating a large batch of never-before-seen products (live calls to
# Google Translate, no timeout) is what made a cold-cache run take 4+ hours in CI once.
# Cap new translations per run; anything over the cap keeps its Dutch name for now and
# gets picked up on a later run once translated. Override via env if you want a full
# one-off backfill locally (e.g. AH_MAX_NEW_TRANSLATIONS=999999 python run.py).
MAX_NEW_TRANSLATIONS_PER_RUN = int(os.getenv("AH_MAX_NEW_TRANSLATIONS", "3000"))

def translate_text_batch(text_list):
    """Translates a list of strings from Dutch to English and returns a dictionary map."""
    translator = GoogleTranslator(source='nl', target='en')
    unique_texts = list(set([t for t in text_list if isinstance(t, str) and t.strip()]))
    
    if not unique_texts:
        return {}

    print(f"   Note: Found {len(unique_texts)} unique terms to translate.")
    translation_map = {}

    for i in range(0, len(unique_texts), BATCH_SIZE):
        batch = unique_texts[i : i + BATCH_SIZE]
        try:
            results = translator.translate_batch(batch)
            for original, translated in zip(batch, results):
                translation_map[original] = translated
                
            print(f"   ... Translated {min(i + BATCH_SIZE, len(unique_texts))}/{len(unique_texts)}")
            time.sleep(0.2)
        except Exception as e:
            print(f"   ❌ Batch Error: {e}")
            for item in batch: 
                translation_map[item] = item # Fallback to original on failure

    return translation_map

def load_translation_memory():
    """Loads memory, handles legacy schemas, and ensures uniqueness."""
    if not os.path.exists(MEMORY_FILE):
        print(f"🆕 No memory file found. Creating: {MEMORY_FILE}")
        df = pd.DataFrame(columns=['store', 'id', 'dutch_title', 'english_title'])
        df.to_csv(MEMORY_FILE, index=False)
        return df
    
    df = pd.read_csv(MEMORY_FILE, dtype={'id': str, 'store': str})
    
    # Schema Migration: Add 'store' column to historical AH data if missing
    if 'store' not in df.columns:
        print("   🔄 Migrating legacy memory file (adding 'store' column)...")
        df.insert(0, 'store', 'albert_heijn')
    
    initial_len = len(df)
    # Deduplicate based on store + id combo
    df = df.drop_duplicates(subset=['store', 'id'], keep='last')
    
    if len(df) < initial_len:
        print(f"   🧹 Cleaned {initial_len - len(df)} duplicate records from memory.")
        df.to_csv(MEMORY_FILE, index=False)
        
    print(f"🧠 Loaded Translation Memory: {len(df)} unique records.")
    return df

def update_memory_safely(new_entries_df):
    """Updates the CSV file efficiently and safely."""
    if new_entries_df.empty: 
        return

    existing_df = load_translation_memory()
    combined_df = pd.concat([existing_df, new_entries_df], ignore_index=True)
    deduped_df = combined_df.drop_duplicates(subset=['store', 'id'], keep='last')
    
    deduped_df.to_csv(MEMORY_FILE, index=False)
    print(f"💾 Saved Memory: Added {len(new_entries_df)} new items. Total memory size: {len(deduped_df)}.")

def process_file(filepath, store_name, id_col, title_col, aisle_col=None):
    """
    Standardized ETL transformer: 
    Finds missing translations, updates memory, joins data, and overwrites the file.
    """
    if not filepath:
        return
        
    print(f"\n🚜 Processing {store_name.upper()} Data: {filepath}")
    try:
        df = pd.read_csv(filepath)
    except FileNotFoundError:
        print(f"❌ File not found: {filepath}")
        return

    # Ensure ID is string for consistent matching
    df[id_col] = df[id_col].astype(str)
    memory_df = load_translation_memory()
    
    # Filter memory for this specific store
    store_memory = memory_df[memory_df['store'] == store_name]
    known_translations = dict(zip(store_memory['id'], store_memory['english_title']))

    # Identify items needing translation
    missing_mask = ~df[id_col].isin(known_translations.keys())
    all_missing = df[missing_mask].drop_duplicates(subset=[id_col])

    # Cap how many NEW products get live-translated this run (see config above).
    # Overflow keeps its Dutch name for now and will be translated on a future run
    # once it falls within the cap (memory grows monotonically, so it converges).
    if len(all_missing) > MAX_NEW_TRANSLATIONS_PER_RUN:
        print(f"   ⚠️ {len(all_missing)} new {store_name} products found, "
              f"capping this run to {MAX_NEW_TRANSLATIONS_PER_RUN} "
              f"(remaining {len(all_missing) - MAX_NEW_TRANSLATIONS_PER_RUN} deferred to a later run).")
        missing_items = all_missing.iloc[:MAX_NEW_TRANSLATIONS_PER_RUN]
    else:
        missing_items = all_missing

    if not missing_items.empty:
        print(f"   Translating {len(missing_items)} new {store_name} products...")
        titles_to_translate = missing_items[title_col].tolist()
        new_translations_map = translate_text_batch(titles_to_translate)
        
        # Build new memory dataframe
        new_memory_data = []
        for _, row in missing_items.iterrows():
            item_id = row[id_col]
            dutch_text = row[title_col]
            english_text = new_translations_map.get(dutch_text, dutch_text)
            
            new_memory_data.append({
                'store': store_name,
                'id': item_id,
                'dutch_title': dutch_text,
                'english_title': english_text
            })
            # Update local dictionary immediately for the join
            known_translations[item_id] = english_text
            
        new_memory_df = pd.DataFrame(new_memory_data)
        update_memory_safely(new_memory_df)

    # Perform the In-Memory Join
    df['title_eng'] = df[id_col].map(known_translations).fillna(df[title_col])
    
    # Handle Aisle/Categories (Small enough to translate on the fly without tracking in memory)
    if aisle_col and aisle_col in df.columns:
        print("   Translating categories...")
        unique_aisles = df[aisle_col].dropna().unique().tolist()
        aisle_map = translate_text_batch(unique_aisles)
        df['aisle_eng'] = df[aisle_col].map(aisle_map).fillna(df[aisle_col])

    # Overwrite the original file
    df.to_csv(filepath, index=False)
    print(f"✅ Data enriched and saved over {filepath}")

def run_translation_pipeline(lidl_file=None, ah_export_file=None, ah_summary_file=None):
    # Lidl uses 'title' as the ID since actual IDs are unstable week-to-week
    if lidl_file: 
        process_file(lidl_file, store_name='lidl', id_col='title', title_col='title')
        
    # Albert Heijn Summary uses the aisle name as the identifier
    if ah_summary_file: 
        process_file(ah_summary_file, store_name='ah_summary', id_col='scraped_aisle', title_col='scraped_aisle')
        
    # Albert Heijn Main Export uses strict product IDs
    if ah_export_file: 
        process_file(ah_export_file, store_name='albert_heijn', id_col='id', title_col='title', aisle_col='scraped_aisle')