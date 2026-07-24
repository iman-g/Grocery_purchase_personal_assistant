from curl_cffi import requests
import xml.etree.ElementTree as ET
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


def get_access_token(session):
    """Fetches an anonymous OAuth token. AH returns XML, not JSON."""
    resp = session.post(
        "https://api.ah.nl/mobile-auth/v1/auth/token/anonymous",
        headers={
            'x-clientid': 'appie',
            'x-clientversion': '8.22.3',
            'content-type': 'application/json',
            'user-agent': 'Appie/8.22.3'
        },
        json={"clientId": "appie"},
        timeout=15
    )
    if resp.status_code != 200:
        raise Exception(f"Auth failed ({resp.status_code}): {resp.text[:200]}")
    root = ET.fromstring(resp.text)
    token = root.findtext('access_token')
    if not token:
        raise Exception(f"No access_token in response: {resp.text[:200]}")
    return token


def build_headers(token):
    return {
        'authorization': f'Bearer {token}',
        'x-clientid': 'appie',
        'x-clientversion': '8.22.3',
        'x-superapplication': 'appie',
        'x-application': 'AHWEBSHOP',
        'accept': 'application/json',
        'user-agent': 'Appie/8.22.3',
        'origin': 'https://www.ah.nl',
        'referer': 'https://www.ah.nl/'
    }


def parse_price_and_discount(p):
    """
    AH has three distinct bonus/price structures:

    1. SIMPLE PRICE REDUCTION (e.g. normally €1.99, now €1.49)
       currentPrice = 1.49, priceBeforeBonus = 1.99
       → final_price = currentPrice, original_price = priceBeforeBonus

    2. MULTI-ITEM DEAL (e.g. "2 voor €0.89", "3 voor €2")
       currentPrice = null, priceBeforeBonus = 0.69 (single unit price)
       discountLabels[0].price = 0.89 (total deal price)
       discountLabels[0].count = 2    (how many units)
       → final_price = deal_price / count (per unit), original_price = priceBeforeBonus
       → discount_label = "2 voor 0.89"

    3. NO DISCOUNT (regular product)
       currentPrice = null, priceBeforeBonus = 0.85
       → final_price = original_price = priceBeforeBonus
    """
    current_price = p.get('currentPrice')       # float or None
    before_bonus = p.get('priceBeforeBonus')    # float or None (regular price)
    is_bonus = p.get('isBonus', False)
    discount_labels = p.get('discountLabels') or []
    bonus_mechanism = p.get('bonusMechanism', '')  # e.g. "2 voor 0.89"

    discount_label = ""
    deal_price = None      # total deal price (e.g. 0.89 for "2 voor 0.89")
    deal_count = None      # number of units in deal
    discount_pct = None

    # Parse discountLabels for multi-item deals
    if discount_labels and isinstance(discount_labels, list):
        dl = discount_labels[0]
        if isinstance(dl, dict):
            deal_price = dl.get('price')
            deal_count = dl.get('count')
            discount_label = dl.get('defaultDescription') or bonus_mechanism or "Bonus"
            pct = dl.get('percentage') or dl.get('precisePercentage')
            if pct:
                discount_pct = round(float(pct), 1)

    # Case 1: Simple price reduction
    if current_price is not None:
        final_price = current_price
        original_price = before_bonus if before_bonus else current_price
        if not discount_label:
            discount_label = bonus_mechanism or ("Bonus" if is_bonus else "")
        if not discount_pct and original_price and original_price != final_price:
            discount_pct = round(100 * (original_price - final_price) / original_price, 1)

    # Case 2: Multi-item deal (currentPrice is null but discountLabels has price)
    elif is_bonus and deal_price is not None and deal_count:
        final_price = round(deal_price / deal_count, 4)   # effective per-unit price
        original_price = before_bonus if before_bonus else final_price
        if not discount_pct and original_price and original_price != final_price:
            discount_pct = round(100 * (original_price - final_price) / original_price, 1)

    # Case 2b: isBonus but no parseable deal price — flag it but keep regular price
    elif is_bonus:
        final_price = before_bonus
        original_price = before_bonus
        discount_label = bonus_mechanism or "Bonus"

    # Case 3: No discount
    else:
        final_price = before_bonus
        original_price = before_bonus

    return final_price, original_price, discount_label, discount_pct, deal_price, deal_count



def scrape_ah_final(max_categories=None, max_pages_per_category=None):
    print("🚀 Initializing AH Session...")
    session = requests.Session(impersonate="chrome110")

    try:
        token = get_access_token(session)
        print(f"    ✅ Token acquired.")
    except Exception as e:
        print(f"❌ Could not get token: {e}")
        return None, None

    session.headers.update(build_headers(token))

    base_url = "https://api.ah.nl/mobile-services/product/search/v2"
    master_data = []
    current_date = datetime.now().strftime("%Y-%m-%d")
    date_clean = datetime.now().strftime("%Y%m%d")

    ACTIVE_CATS = {k: v for k, v in CATEGORIES.items() if not any(b in k for b in BLACKLIST_KEYWORDS)}
    
    # --- SCALE CONTROL: Limit the number of categories ---
    cat_items = list(ACTIVE_CATS.items())
    if max_categories is not None:
        cat_items = cat_items[:max_categories]
        print(f"\n🚜 Starting Bounded Scrape of {len(cat_items)} Categories (Limited from {len(ACTIVE_CATS)})...")
    else:
        print(f"\n🚜 Starting Full Scrape of {len(ACTIVE_CATS)} Categories...")

    for slug, tax_id in cat_items:
        print(f"\n    📂 {slug} (ID: {tax_id})")
        page = 0
        total_pages = 1
        products_collected = 0
        bonus_found = 0

        while page < total_pages:
            # --- SCALE CONTROL: Limit the pages per category ---
            if max_pages_per_category is not None and page >= max_pages_per_category:
                print(f"    Stopping category early at page limit ({max_pages_per_category}).")
                break

            params = {'sortOn': 'RELEVANCE', 'taxonomyId': tax_id, 'page': page, 'size': 36}
            try:
                resp = session.get(base_url, params=params, timeout=15)

                if resp.status_code == 401:
                    print("    🔄 Token expired, refreshing...")
                    token = get_access_token(session)
                    session.headers.update(build_headers(token))
                    resp = session.get(base_url, params=params, timeout=15)

                if resp.status_code != 200:
                    print(f"    ⚠️ Status {resp.status_code} on page {page}, skipping.")
                    break

                data = resp.json()

                if page == 0:
                    total_pages = data.get('page', {}).get('totalPages', 1)

                products = data.get('products', [])
                if not products:
                    break

                for p in products:
                    try:
                        final_price, original_price, discount_label, discount_pct, deal_price, deal_count = \
                            parse_price_and_discount(p)

                        if p.get('isBonus'):
                            bonus_found += 1

                        master_data.append({
                            "id": str(p.get('webshopId', '')),
                            "hq_id": str(p.get('hqId', '')),
                            "title": p.get('title'),
                            "brand": p.get('brand'),
                            "scraped_aisle": slug,
                            "main_category": p.get('mainCategory'),
                            "sub_category": p.get('subCategory'),
                            "final_price": final_price,
                            "original_price": original_price,
                            "discount_pct": discount_pct,
                            "discount_label": discount_label,
                            "deal_price": deal_price,
                            "deal_count": deal_count,
                            "unit": p.get('salesUnitSize'),
                            "unit_price_description": p.get('unitPriceDescription'),
                            "is_bonus": p.get('isBonus', False),
                            "discount_type": p.get('discountType'),
                            "bonus_mechanism": p.get('bonusMechanism'),
                            "bonus_start": p.get('bonusStartDate'),
                            "bonus_end": p.get('bonusEndDate'),
                            "nutriscore": p.get('nutriscore'),
                            "available_online": p.get('availableOnline'),
                            "shop_type": p.get('shopType'),
                            "url": f"https://www.ah.nl/producten/product/wi{p.get('webshopId', '')}",
                            "scraped_at": current_date
                        })
                        products_collected += 1
                    except Exception:
                        continue

                page += 1
                time.sleep(random.uniform(0.4, 0.9))

            except Exception as e:
                print(f"    ❌ Error on page {page}: {e}")
                break

        print(f"      ✅ Found {products_collected} items ({bonus_found} on bonus).")
        time.sleep(1.0)

    if not master_data:
        print("⚠️ No products collected.")
        return None, None

    print(f"\n📊 Processing AH Data ({len(master_data)} total records)...")
    df = pd.DataFrame(master_data)
    df = df.dropna(subset=['id', 'title'])

    summary_df = df.groupby('scraped_aisle').agg(
        items_found=('id', 'count'),
        bonus_items=('is_bonus', 'sum')
    ).reset_index()
    summary_filename = f"ah_summary1_{date_clean}.csv"
    summary_df.to_csv(summary_filename, index=False)
    print(f"    📄 Summary saved: {summary_filename}")

    df_unique = df.drop_duplicates(subset=['id'])
    export_filename = f"ah_full_export1_{date_clean}.csv"
    df_unique.to_csv(export_filename, index=False)
    print(f"    💾 Export saved: {export_filename}")
    print(f"      {len(df_unique)} unique products, {int(df_unique['is_bonus'].sum())} currently on bonus/discount")

    return export_filename, summary_filename


if __name__ == "__main__":
    # Change the execution lines below based on your testing phase
    
    # PHASE A: 1 Category, 1 Page (Tiny Smoke Test)
    # scrape_ah_final(max_categories=1, max_pages_per_category=1)
    
    # PHASE B: 3 Categories, 2 Pages each (Performance/Rate-limit Test)
    # scrape_ah_final(max_categories=3, max_pages_per_category=2)
    
    # PHASE C: Full Production Run
    scrape_ah_final()