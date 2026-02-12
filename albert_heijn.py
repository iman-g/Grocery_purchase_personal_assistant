from curl_cffi import requests
import pandas as pd
import time
import random
from datetime import datetime

# --- CONFIG ---
CATEGORIES = {
        'vegetarisch-vegan-en-plantaardig': '20128',
        'groente-aardappelen': '6401', 
        'fruit-verse-sappen': '20885', 
        'maaltijden-salades': '1301', 
        'vlees': '9344', 
        'vis': '1651', 
        'vleeswaren': '5481',
        'kaas': '1192', 
        'zuivel-eieren': '1730',
        'bakkerij': '1355',
        'glutenvrij': '4246',
        'borrel-chips-snacks': '20824',
        'pasta-rijst-wereldkeuken': '1796',
        'soepen-sauzen-kruiden-olie': '6409',
        'koek-snoep-chocolade': '20129',
        'ontbijtgranen-beleg': '6405',
        'tussendoortjes': '2457',
        'diepvries': '5881',
        'koffie-thee': '1043',
        'frisdrank-sappen-water': '20130',
        'bier-wijn-aperitieven': '6406',
        'drogisterij': '1045',
        'gezondheid-en-sport': '11717',
        'huishouden': '1165',
        'koken-tafelen-vrije-tijd': '1057'
    }
    
BLACKLIST_KEYWORDS = ['baby', 'kind', 'huisdier', 'dier']

def scrape_ah_final():
    print("🚀 Initializing AH Session...")
    session = requests.Session(impersonate="chrome110")
    session.headers.update({
        'Host': 'www.ah.nl',
        'accept': 'application/json',
        'referer': 'https://www.ah.nl/producten',
        'x-requested-with': 'XMLHttpRequest'
    })

    base_url = "https://www.ah.nl/zoeken/api/products/search"
    master_data = []
    current_date = datetime.now().strftime("%Y-%m-%d")
    date_clean = datetime.now().strftime("%Y%m%d")

    ACTIVE_CATS = {k: v for k, v in CATEGORIES.items() if not any(b in k for b in BLACKLIST_KEYWORDS)}
    print(f"\n🚜 Starting Scrape of {len(ACTIVE_CATS)} Official Root Categories...")

    for slug, tax_id in ACTIVE_CATS.items():
        print(f"\n   📂 {slug} (ID: {tax_id})")
        page = 0
        total_pages = 1
        products_collected = 0
        
        while page < total_pages:
            params = {'taxonomy': tax_id, 'size': 36, 'page': page}
            try:
                resp = session.get(base_url, params=params, timeout=15)
                if resp.status_code != 200: break
                
                data = resp.json()
                if page == 0: total_pages = data.get('page', {}).get('totalPages', 1)

                cards = data.get('cards', [])
                for card in cards:
                    for p in card.get('products', []):
                        try:
                            price_obj = p.get('price', {})
                            final_price = price_obj.get('now')
                            was_price = price_obj.get('was')
                            original_price = was_price if was_price else final_price
                            discount = p.get('shield', {}).get('text', "")
                            if not discount and p.get('discount'): discount = "Bonus"

                            item = {
                                "id": p.get('id'),
                                "title": p.get('title'),
                                "scraped_aisle": slug,
                                "category_specific": p.get('category'),
                                "final_price": final_price,
                                "original_price": original_price,
                                "unit": price_obj.get('unitSize'),
                                "discount": discount,
                                "nutriscore": p.get('properties', {}).get('nutriscore'),
                                "url": f"https://www.ah.nl{p.get('link', '')}",
                                "scraped_at": current_date
                            }
                            master_data.append(item)
                            products_collected += 1
                        except: continue
                page += 1
                time.sleep(random.uniform(0.6, 1.2))
            except Exception: break
        print(f"      ✅ Found {products_collected} items.")
        time.sleep(2)

    if master_data:
        print(f"\n📊 Processing AH Data...")
        df = pd.DataFrame(master_data)
        
        # 1. Summary
        summary_df = df.groupby(['scraped_aisle']).size().reset_index(name='items_found')
        summary_filename = f"ah_summary1_{date_clean}.csv"
        summary_df.to_csv(summary_filename, index=False)
        print(f"   📄 Summary saved: {summary_filename}")

        # 2. Overlaps
        aisle_map = df.groupby('id')['scraped_aisle'].apply(lambda x: '; '.join(sorted(set(x)))).reset_index()
        aisle_map.rename(columns={'scraped_aisle': 'all_aisles'}, inplace=True)
        df = df.merge(aisle_map, on='id', how='left')
        
        # 3. Unique Export
        df_unique = df.drop_duplicates(subset=['id'])
        export_filename = f"ah_full_export1_{date_clean}.csv"
        df_unique.to_csv(export_filename, index=False)
        print(f"   💾 Export saved: {export_filename}")
        
        return export_filename, summary_filename
    else:
        return None, None

if __name__ == "__main__":
    scrape_ah_final()