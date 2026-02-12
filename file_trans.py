import pandas as pd
from deep_translator import GoogleTranslator
import time
import os

# --- CONFIGURATION ---
BATCH_SIZE = 50
MEMORY_FILE = "product_translation_memory.csv"

def translate_text_batch(text_list):
    """
    Translates a list of strings from Dutch to English.
    """
    translator = GoogleTranslator(source='nl', target='en')
    # Filter out non-strings or empty strings
    unique_texts = list(set([t for t in text_list if isinstance(t, str) and t.strip()]))
    translation_map = {}
    
    if not unique_texts:
        return text_list

    print(f"   Note: Found {len(unique_texts)} unique terms to translate.")

    for i in range(0, len(unique_texts), BATCH_SIZE):
        batch = unique_texts[i : i + BATCH_SIZE]
        try:
            results = translator.translate_batch(batch)
            for original, translated in zip(batch, results):
                translation_map[original] = translated
            
            # Progress bar effect
            print(f"   ... Translated {min(i + BATCH_SIZE, len(unique_texts))}/{len(unique_texts)}")
            time.sleep(0.2)
        except Exception as e:
            print(f"   ❌ Batch Error: {e}")
            for item in batch: translation_map[item] = item # Fallback

    # Map back to original list
    return [translation_map.get(t, t) if isinstance(t, str) else t for t in text_list]

def load_translation_memory():
    """
    Loads memory and ENSURES uniqueness.
    """
    if not os.path.exists(MEMORY_FILE):
        print(f"🆕 No memory file found. Creating new one: {MEMORY_FILE}")
        df = pd.DataFrame(columns=['id', 'dutch_title', 'english_title'])
        df.to_csv(MEMORY_FILE, index=False)
        return {}
    
    print(f"🧠 Loading Translation Memory from {MEMORY_FILE}...")
    # Force ID to string
    df = pd.read_csv(MEMORY_FILE, dtype={'id': str})
    
    # CRITICAL FIX: Drop duplicates immediately upon loading
    initial_len = len(df)
    df = df.drop_duplicates(subset=['id'], keep='last')
    if len(df) < initial_len:
        print(f"   🧹 Cleaned {initial_len - len(df)} duplicate IDs from memory file.")
        # Optional: Save clean version back immediately
        df.to_csv(MEMORY_FILE, index=False)
    
    # Create lookup dictionary
    memory_dict = pd.Series(df.english_title.values, index=df.id).to_dict()
    print(f"   ↳ Loaded {len(memory_dict)} unique translated items.")
    return memory_dict

def update_memory_safely(new_entries_df):
    """
    Updates the CSV file while strictly enforcing uniqueness.
    """
    if new_entries_df.empty: return

    # 1. Load existing file
    if os.path.exists(MEMORY_FILE):
        existing_df = pd.read_csv(MEMORY_FILE, dtype={'id': str})
    else:
        existing_df = pd.DataFrame(columns=['id', 'dutch_title', 'english_title'])

    # 2. Combine Old + New
    combined_df = pd.concat([existing_df, new_entries_df], ignore_index=True)

    # 3. Deduplicate (Keep LAST aka newest entry if there's a conflict)
    deduped_df = combined_df.drop_duplicates(subset=['id'], keep='last')

    # 4. Save (Overwrite the file with the clean version)
    deduped_df.to_csv(MEMORY_FILE, index=False)
    print(f"💾 Updated Memory: File now contains {len(deduped_df)} unique items (Added {len(new_entries_df)} new).")

def process_lidl(filename):
    print(f"\n🚜 Processing Lidl File: {filename}...")
    try:
        df = pd.read_csv(filename)
        # Lidl doesn't use the ID memory because IDs aren't stable across weeks
        if 'title' in df.columns:
            print("   Translating 'title'...")
            df['title_eng'] = translate_text_batch(df['title'].tolist())
        
        output_name = filename.replace(".csv", "_translated.csv")
        df.to_csv(output_name, index=False)
        print(f"✅ Saved to {output_name}")
    except FileNotFoundError: print(f"❌ File not found: {filename}")

def process_ah_summary(filename):
    print(f"\n🚜 Processing AH Summary: {filename}...")
    try:
        df = pd.read_csv(filename)
        # Summary categories are small, just translate on the fly
        if 'scraped_aisle' in df.columns:
            df['aisle_eng'] = translate_text_batch(df['scraped_aisle'].tolist())
        
        output_name = filename.replace(".csv", "_translated.csv")
        df.to_csv(output_name, index=False)
        print(f"✅ Saved to {output_name}")
    except FileNotFoundError: print(f"❌ File not found: {filename}")

def process_ah_export(filename):
    print(f"\n🚀 Processing AH Export: {filename}")
    try:
        df_daily = pd.read_csv(filename)
    except FileNotFoundError:
        print("❌ Input file not found.")
        return

    # 1. Load Memory (Cleaned)
    memory_map = load_translation_memory()
    
    # Ensure ID is string
    df_daily['id'] = df_daily['id'].astype(str)
    
    # 2. Identify Missing Translations
    # Only look for IDs that are NOT in our memory map
    is_new = ~df_daily['id'].isin(memory_map.keys())
    new_products = df_daily[is_new].copy()
    
    print(f"   New Items to Translate: {len(new_products)}")
    
    # 3. Translate Only New Items
    if not new_products.empty:
        # Deduplicate within the new batch itself (e.g. if 'Milk' appears twice in today's file)
        unique_new = new_products[['id', 'title']].drop_duplicates(subset=['id'])
        
        titles_nl = unique_new['title'].tolist()
        titles_en = translate_text_batch(titles_nl)
        
        # Prepare new memory dataframe
        new_memory = pd.DataFrame({
            'id': unique_new['id'],
            'dutch_title': titles_nl,
            'english_title': titles_en
        })
        
        # Save safely
        update_memory_safely(new_memory)
        
        # Update local map for this run
        new_map = pd.Series(titles_en, index=unique_new['id']).to_dict()
        memory_map.update(new_map)
    
    # 4. Map Translations to Main Dataframe
    df_daily['title_eng'] = df_daily['id'].map(memory_map).fillna(df_daily['title'])
    
    # 5. Translate Categories (Optional, usually small enough to just run)
    if 'scraped_aisle' in df_daily.columns:
        df_daily['aisle_eng'] = translate_text_batch(df_daily['scraped_aisle'].tolist())

    output_file = filename.replace(".csv", "_translated.csv")
    df_daily.to_csv(output_file, index=False)
    print(f"✅ Finished! Saved to {output_file}")

def run_translation_pipeline(lidl_file=None, ah_export_file=None, ah_summary_file=None):
    if lidl_file: process_lidl(lidl_file)
    if ah_summary_file: process_ah_summary(ah_summary_file)
    if ah_export_file: process_ah_export(ah_export_file)

if __name__ == "__main__":
    print("Run via run.py please.")
