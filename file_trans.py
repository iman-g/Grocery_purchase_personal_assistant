import pandas as pd
from deep_translator import GoogleTranslator
import time
import os

# --- CONFIGURATION ---
BATCH_SIZE = 50
MEMORY_FILE = "product_translation_memory.csv"

def translate_text_batch(text_list):
    translator = GoogleTranslator(source='nl', target='en')
    unique_texts = list(set([t for t in text_list if isinstance(t, str) and t.strip()]))
    translation_map = {}
    print(f"   Note: Found {len(unique_texts)} unique terms to translate.")

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
            for item in batch: translation_map[item] = item # Fallback

    return [translation_map.get(t, t) if isinstance(t, str) else t for t in text_list]

def load_translation_memory():
    if not os.path.exists(MEMORY_FILE):
        print(f"🆕 No memory file found. Creating new one: {MEMORY_FILE}")
        df = pd.DataFrame(columns=['id', 'dutch_title', 'english_title'])
        df.to_csv(MEMORY_FILE, index=False)
        return {}
    
    print(f"🧠 Loading Translation Memory from {MEMORY_FILE}...")
    df = pd.read_csv(MEMORY_FILE, dtype={'id': str}) 
    
    df['id'] = df['id'].astype(str)

    memory_dict = pd.Series(df.english_title.values, index=df.id).to_dict()
    print(f"   ↳ Loaded {len(memory_dict)} previously translated items.")
    return memory_dict

def update_memory(new_translations_df):
    if new_translations_df.empty: return
    header = not os.path.exists(MEMORY_FILE)
    new_translations_df.to_csv(MEMORY_FILE, mode='a', header=header, index=False)
    print(f"💾 Updated Memory: Added {len(new_translations_df)} new items.")

def process_lidl(filename):
    print(f"\n🚜 Processing Lidl File: {filename}...")
    try:
        df = pd.read_csv(filename)
        if 'title' in df.columns:
            print("   Translating 'title'...")
            df['title_eng'] = translate_text_batch(df['title'].tolist())
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

    memory_map = load_translation_memory()
    df_daily['id'] = df_daily['id'].astype(str)
    
    is_new = ~df_daily['id'].isin(memory_map.keys())
    new_products = df_daily[is_new].copy()
    
    print(f"   New Items to Translate: {len(new_products)}")
    
    if not new_products.empty:
        unique_new = new_products[['id', 'title']].drop_duplicates(subset=['id'])
        titles_nl = unique_new['title'].tolist()
        titles_en = translate_text_batch(titles_nl)
        
        new_memory = pd.DataFrame({'id': unique_new['id'], 'dutch_title': titles_nl, 'english_title': titles_en})
        update_memory(new_memory)
        memory_map.update(pd.Series(titles_en, index=unique_new['id']).to_dict())
    
    df_daily['title_eng'] = df_daily['id'].map(memory_map).fillna(df_daily['title'])
    
    # Also translate categories if present
    if 'scraped_aisle' in df_daily.columns:
        df_daily['aisle_eng'] = translate_text_batch(df_daily['scraped_aisle'].tolist())

    output_file = filename.replace(".csv", "_translated.csv")
    df_daily.to_csv(output_file, index=False)
    print(f"✅ Finished! Saved to {output_file}")

def process_ah_summary(filename):
    print(f"\n🚜 Processing AH Summary: {filename}...")
    try:
        df = pd.read_csv(filename)
        if 'scraped_aisle' in df.columns:
            df['aisle_eng'] = translate_text_batch(df['scraped_aisle'].tolist())
        output_name = filename.replace(".csv", "_translated.csv")
        df.to_csv(output_name, index=False)
        print(f"✅ Saved to {output_name}")
    except FileNotFoundError: print(f"❌ File not found: {filename}")

def run_translation_pipeline(lidl_file=None, ah_export_file=None, ah_summary_file=None):
    if lidl_file: process_lidl(lidl_file)
    if ah_summary_file: process_ah_summary(ah_summary_file)
    if ah_export_file: process_ah_export(ah_export_file)

if __name__ == "__main__":
    print("Run via run_pipeline.py please.")