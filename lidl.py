from curl_cffi import requests
from lxml import html
import json
import pandas as pd
import html as html_parser
from datetime import datetime

def scrape_lidl_final_refined():
    base_url = "https://www.lidl.nl/c/aanbiedingen/a10008785"
    print(f"🚀 Scraping Lidl Offers: {base_url}")
    session = requests.Session(impersonate="chrome110")
    
    offers_data = []
    
    try:
        print("   ⏳ Requesting page...", end="")
        response = session.get(base_url, timeout=30)
        if response.status_code != 200:
            print(f" ❌ Failed: {response.status_code}")
            return None
        print(" ✅ Done.")

        tree = html.fromstring(response.content)
        grid_nodes = tree.xpath('//*[@data-grid-data]/@data-grid-data')
        
        if not grid_nodes:
            print("   ⚠️ No data found.")
            return None

        for raw_json in grid_nodes:
            try:
                clean_json = html_parser.unescape(raw_json)
                data = json.loads(clean_json)
                if isinstance(data, dict): data = [data]
                
                for p in data:
                    if not isinstance(p, dict) or 'fullTitle' not in p: continue

                    # --- PRICE LOGIC ---
                    raw_current = None
                    raw_old = None
                    discount_label = ""
                    source_type = "Standard"

                    # Lidl Plus
                    lidl_plus_data = p.get('lidlPlus')
                    if lidl_plus_data and isinstance(lidl_plus_data, list) and len(lidl_plus_data) > 0:
                        lp_item = lidl_plus_data[0]
                        lp_price_obj = lp_item.get('price', {})
                        if lp_price_obj.get('price'):
                            raw_current = lp_price_obj.get('price')
                            raw_old = lp_price_obj.get('oldPrice')
                            discount_label = "Lidl Plus"
                            source_type = "Member Deal"
                            if lp_item.get('highlightText'):
                                discount_label += f" ({lp_item.get('highlightText')})"

                    # Standard Fallback
                    if raw_current is None:
                        std_price_obj = p.get('price', {})
                        raw_current = std_price_obj.get('price')
                        raw_old = std_price_obj.get('oldPrice')
                        if not raw_current: raw_current = p.get('priceLabel')

                    try:
                        final_price = float(raw_current) if raw_current else 0.0
                        old_price = float(raw_old) if raw_old else 0.0
                    except ValueError: continue

                    if old_price == 0: old_price = final_price

                    # Discount Calc
                    discount_pct = 0
                    if old_price > 0 and old_price != final_price:
                        try:
                            val = 100 * (old_price - final_price) / old_price
                            discount_pct = int(round(val))
                        except ZeroDivisionError: discount_pct = 0

                    if "Lidl Plus" not in discount_label:
                        ribbons = p.get('ribbons')
                        if ribbons and isinstance(ribbons, list) and len(ribbons) > 0:
                            discount_label = ribbons[0].get('text', '')
                        elif p.get('merchandising', {}).get('text'):
                             discount_label = p.get('merchandising', {}).get('text')

                    item = {
                        "title": p.get('fullTitle'),
                        "price": final_price,
                        "old_price": old_price,
                        "discount_percent": discount_pct,
                        "discount_label": discount_label,
                        "deal_type": source_type,
                        "unit": p.get('price', {}).get('packaging', {}).get('text') or p.get('price', {}).get('unitSize'),
                        "url": f"https://www.lidl.nl{p.get('canonicalUrl', '')}",
                        "scraped_at": datetime.now().strftime("%Y-%m-%d")
                    }
                    offers_data.append(item)
            except Exception: continue

    except Exception as e:
        print(f"\n❌ Error: {e}")
        return None

    if offers_data:
        df = pd.DataFrame(offers_data)
        df = df.drop_duplicates(subset=['title'])
        filename = f"lidl_offers_{datetime.now().strftime('%Y%m%d')}.csv"
        df.to_csv(filename, index=False)
        print(f"✅ Lidl Scraper Finished: {filename} ({len(df)} offers)")
        return filename
    else:
        print("⚠️ No products extracted.")
        return None

if __name__ == "__main__":
    scrape_lidl_final_refined()