
# """
# Grocery Receipt Telegram Bot
# ------------------------------
# Send a receipt photo → Gemini Vision parses it → appends rows to Google Sheet.

# Setup:
#   pip install python-telegram-bot google-genai gspread

# Required files in same folder:
#   grocery_tracker.json   ← Google service account credentials

# Required values in CONFIG below:
#   TELEGRAM_TOKEN   - from @BotFather
#   GEMINI_API_KEY   - from aistudio.google.com/apikey
#   SHEET_ID         - Google Sheet ID from the URL
# """

# import os
# import ssl
# import certifi

# # 1. Point standard SSL verification to certifi before any network libraries load
# os.environ["SSL_CERT_FILE"] = certifi.where()

# # 2. Monkey-patch Python's SSL context to survive malformed Windows certificates
# _original_load_windows_store_certs = ssl.SSLContext._load_windows_store_certs
# def _safe_load_windows_store_certs(self, storename, purpose):
#     try:
#         _original_load_windows_store_certs(self, storename, purpose)
#     except ssl.SSLError as e:
#         if "ASN1" in str(e):
#             pass # Ignore the bad certificate and continue loading the rest
#         else:
#             raise
# ssl.SSLContext._load_windows_store_certs = _safe_load_windows_store_certs

# # 3. Now it is safe to import Telegram and other libraries
# import json
# import logging
# import re
# from datetime import datetime

# from telegram import Update
# from telegram.ext import ApplicationBuilder, MessageHandler, CommandHandler, filters, ContextTypes
# from google import genai
# from google.genai import types
# import gspread
# from dotenv import load_dotenv

# load_dotenv()

# os.environ["SSL_CERT_FILE"] = certifi.where()


# # ---------------------------------------------------------------------------
# # CONFIG — fill these in
# # ---------------------------------------------------------------------------

# TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
# GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
# SHEET_ID = os.getenv("SHEET_ID")
# SHEET_TAB       = "Raw"
# CREDENTIALS_FILE = "grocery_tracker.json"

# STORE_ID_MAP = {
#     "albert heijn": "albert_heijn",
#     "ah":           "albert_heijn",
#     "lidl":         "lidl",
#     "jumbo":        "jumbo",
#     "plus":         "plus",
#     "aldi":         "aldi",
#     "dirk":         "dirk",
#     "hoogvliet":    "hoogvliet",
#     "ekoplaza":     "ekoplaza",
# }

# # ---------------------------------------------------------------------------
# # LOGGING
# # ---------------------------------------------------------------------------
# logging.basicConfig(
#     format="%(asctime)s | %(levelname)s | %(message)s",
#     level=logging.INFO
# )
# log = logging.getLogger(__name__)

# # ---------------------------------------------------------------------------
# # GOOGLE SHEETS
# # ---------------------------------------------------------------------------
# def get_worksheet():
#     gc = gspread.service_account(filename=CREDENTIALS_FILE)
#     sh = gc.open_by_key(SHEET_ID)
#     return sh.worksheet(SHEET_TAB)

# def append_rows_to_sheet(rows: list[dict]):
#     ws = get_worksheet()
#     headers = ws.row_values(1)
#     if not headers:
#         headers = ["product_original", "product_english", "quantity", "price",
#                    "discount", "date", "storeid", "id", "ids"]
#         ws.append_row(headers)

#     formatted_rows = [[row.get(h, "") for h in headers] for row in rows]
#     ws.append_rows(formatted_rows, value_input_option="USER_ENTERED")
#     log.info(f"Appended {len(formatted_rows)} rows to sheet.")

# # ---------------------------------------------------------------------------
# # GEMINI VISION — parse receipt image (new google-genai SDK)
# # ---------------------------------------------------------------------------
# RECEIPT_PROMPT = """
# You are a receipt parser. Analyze this grocery receipt image and extract every purchased item.

# Return ONLY a valid JSON array. No explanation, no markdown, no code fences — just the raw JSON array.

# Each element must have exactly these fields:
# {
#   "product_original": "exact product name as printed on receipt (in original language)",
#   "quantity": <number, default 1 if not shown>,
#   "price": <price per unit as float, e.g. 1.49>,
#   "discount": "<discount text if any, else empty string>",
#   "date": "<receipt date as DD-MM-YYYY, or today if not visible>",
#   "store": "<store name, e.g. Albert Heijn, Lidl, Jumbo>"
# }

# Rules:
# - price must be the per-unit price (total / quantity if needed)
# - If a product has a discount shown (e.g. -0.50, Bonus, 2e Halve Prijs), put it in discount
# - date format must be DD-MM-YYYY
# - If you cannot read something clearly, make your best guess
# - Do NOT include subtotals, totals, bags, deposit, or non-product lines
# """

# def parse_receipt_with_gemini(image_bytes: bytes) -> list[dict]:
#     client = genai.Client(api_key=GEMINI_API_KEY)

#     response = client.models.generate_content(
#         model="models/gemini-2.5-flash",
#         contents=[
#             RECEIPT_PROMPT,
#             types.Part.from_bytes(data=image_bytes, mime_type="image/jpeg")
#         ]
#     )

#     raw = response.text.strip()
#     # Strip markdown fences if model adds them anyway
#     raw = re.sub(r"^```[a-z]*\n?", "", raw, flags=re.MULTILINE)
#     raw = re.sub(r"\n?```$", "", raw, flags=re.MULTILINE)
#     raw = raw.strip()

#     items = json.loads(raw)
#     return items if isinstance(items, list) else []


# def normalize_items(items: list[dict]) -> list[dict]:
#     today = datetime.now().strftime("%d-%m-%Y")
#     normalized = []
#     for item in items:
#         store_raw = str(item.get("store", "")).lower().strip()
#         storeid = next(
#             (v for k, v in STORE_ID_MAP.items() if k in store_raw),
#             store_raw.replace(" ", "_")
#         )
#         normalized.append({
#             "product_original": item.get("product_original", ""),
#             "product_english":  "",
#             "quantity":         item.get("quantity", 1),
#             "price":            item.get("price", 0),
#             "discount":         item.get("discount", ""),
#             "date":             item.get("date") or today,
#             "storeid":          storeid,
#             "id":               "",
#             "ids":              "",
#         })
#     return normalized

# # ---------------------------------------------------------------------------
# # TELEGRAM HANDLERS
# # ---------------------------------------------------------------------------
# async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
#     await update.message.reply_text(
#         "👋 Hi! I'm your grocery receipt bot.\n\n"
#         "📸 Send me a photo of any grocery receipt and I'll log it to your sheet automatically.\n\n"
#         "Supported stores: Albert Heijn, Lidl, Jumbo, Plus, Aldi, Dirk, and more.\n\n"
#         "Commands:\n"
#         "/start — show this message\n"
#         "/status — check sheet connection"
#     )

# async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
#     try:
#         ws = get_worksheet()
#         count = len(ws.get_all_values()) - 1
#         await update.message.reply_text(f"✅ Sheet connected. {count} purchases logged so far.")
#     except Exception as e:
#         await update.message.reply_text(f"❌ Sheet connection failed: {e}")

# async def handle_photo(update: Update, context: ContextTypes.DEFAULT_TYPE):
#     msg = update.message
#     await msg.reply_text("📸 Receipt received! Parsing with Gemini Vision...")

#     try:
#         # Download highest-res photo
#         photo = msg.photo[-1]
#         file = await context.bot.get_file(photo.file_id)
#         image_bytes = await file.download_as_bytearray()

#         await msg.reply_text("🔍 Analyzing receipt...")

#         items = parse_receipt_with_gemini(bytes(image_bytes))

#         if not items:
#             await msg.reply_text(
#                 "⚠️ Could not extract any products.\n"
#                 "Make sure the photo is clear and well-lit."
#             )
#             return

#         rows = normalize_items(items)
#         append_rows_to_sheet(rows)

#         store = rows[0]["storeid"].replace("_", " ").title() if rows else "Unknown"
#         date  = rows[0]["date"] if rows else "?"
#         lines = [f"✅ Logged *{len(rows)} items* from *{store}* ({date}):\n"]
#         for r in rows:
#             price_str = f"€{float(r['price']):.2f}" if r['price'] else "?"
#             disc_str  = f" _{r['discount']}_" if r['discount'] else ""
#             lines.append(f"• {r['product_original']} × {r['quantity']} — {price_str}{disc_str}")

#         await msg.reply_text("\n".join(lines), parse_mode="Markdown")

#     except json.JSONDecodeError:
#         await msg.reply_text("⚠️ Gemini returned an unexpected format. Try again with a clearer photo.")
#     except Exception as e:
#         log.error(f"Error processing receipt: {e}", exc_info=True)
#         await msg.reply_text(f"❌ Error: {e}")

# async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
#     await update.message.reply_text(
#         "📸 Please send a *photo* of your receipt.",
#         parse_mode="Markdown"
#     )

# # ---------------------------------------------------------------------------
# # MAIN
# # ---------------------------------------------------------------------------
# def main():
#     log.info("Starting Grocery Receipt Bot...")

#     missing = []
#     if TELEGRAM_TOKEN == "YOUR_TELEGRAM_BOT_TOKEN": missing.append("TELEGRAM_TOKEN")
#     if GEMINI_API_KEY == "YOUR_GEMINI_API_KEY":      missing.append("GEMINI_API_KEY")
#     if SHEET_ID       == "YOUR_GOOGLE_SHEET_ID":     missing.append("SHEET_ID")
#     if missing:
#         raise ValueError(f"Please fill in these values in the CONFIG section: {missing}")

#     app = ApplicationBuilder().token(TELEGRAM_TOKEN).build()
#     app.add_handler(CommandHandler("start",  cmd_start))
#     app.add_handler(CommandHandler("status", cmd_status))
#     app.add_handler(MessageHandler(filters.PHOTO, handle_photo))
#     app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))

#     log.info("Bot is running. Send /start in Telegram to test.")
#     app.run_polling(drop_pending_updates=True)

# if __name__ == "__main__":
#     main()



"""
Grocery Receipt Telegram Bot (multi-store)
-------------------------------------------
Supports Albert Heijn and Lidl receipts with custom parsing logic.

Setup:
  pip install python-telegram-bot google-genai gspread python-dotenv

Required files:
  grocery_tracker.json   <- Google service account credentials
  .env                   <- environment variables

Environment variables:
  TELEGRAM_BOT_TOKEN
  GEMINI_API_KEY
  SHEET_ID
"""

import os
import ssl
import certifi

# 1. Fix SSL before any network libraries load
os.environ["SSL_CERT_FILE"] = certifi.where()

# 2. Monkey-patch Windows certificate store to ignore bad certs
_original_load_windows_store_certs = ssl.SSLContext._load_windows_store_certs
def _safe_load_windows_store_certs(self, storename, purpose):
    try:
        _original_load_windows_store_certs(self, storename, purpose)
    except ssl.SSLError as e:
        if "ASN1" in str(e):
            pass
        else:
            raise
ssl.SSLContext._load_windows_store_certs = _safe_load_windows_store_certs

import json
import logging
import re
from datetime import datetime

from telegram import Update
from telegram.ext import ApplicationBuilder, MessageHandler, CommandHandler, filters, ContextTypes
from google import genai
from google.genai import types
import gspread
from dotenv import load_dotenv

load_dotenv()
os.environ["SSL_CERT_FILE"] = certifi.where()

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
SHEET_ID = os.getenv("SHEET_ID")
SHEET_TAB = "Raw"
CREDENTIALS_FILE = "grocery_tracker.json"

STORE_ID_MAP = {
    "albert heijn": "albert_heijn",
    "ah":           "albert_heijn",
    "lidl":         "lidl",
    "jumbo":        "jumbo",
    "plus":         "plus",
    "aldi":         "aldi",
    "dirk":         "dirk",
    "hoogvliet":    "hoogvliet",
    "ekoplaza":     "ekoplaza",
}

# ---------------------------------------------------------------------------
# LOGGING
# ---------------------------------------------------------------------------
logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(message)s",
    level=logging.INFO
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# STORE-SPECIFIC PROMPTS
# ---------------------------------------------------------------------------

AH_RECEIPT_PROMPT = """
You are a receipt parser for **Albert Heijn** grocery receipts.
Your task is to extract every purchased item, plus the receipt metadata, and return a **valid JSON array** with the exact fields described below.

**Receipt layout (Albert Heijn)**
- Product lines appear in a table with columns: `AANTAL` (quantity), `OMSCHRIJVING` (product name), `PRIJS` (unit price), `BEDRAG` (total price for that line).
- The date and time are usually printed near the bottom, e.g. "14-5-2026" and "19:14".
- The merchant identifier is a number after "POI:" or "POL", e.g. "POI: 50078302" -> merchant = "50078302".
- Bonus discounts appear near the bottom (between SUBTOTAAL and UW VOORDEEL) and map to specific items, e.g., "BONUS QUAKERCRUESL -1,59" maps to the "QUAK CRUESLI" product.

**Extraction steps**

1. **Find the receipt metadata**
   - `date` – format `DD-MM-YYYY`.
   - `time` – format `HH:MM` (24-hour).
   - `merchant` – the number after "POI:" or "POL".

2. **Parse product lines**
   For each product row, extract:
   - `quantity` – the number in the `AANTAL` column (default 1).
   - `product_original` – the exact product name from `OMSCHRIJVING`.
   - `price` – the unit price from `PRIJS` (as a float, convert commas to dots).
   - `total_price` – the total price for that line from `BEDRAG` (float).

3. **Exclude non-product lines**
   Exclude subtotals, totals, VAT, payment lines, deposit (STATIEGELD, EMBALLAGE), and the bonus summary lines themselves.

4. **Map and Distribute Discounts**
   - Look for the bonus deduction lines at the bottom (e.g., "BONUS PEP/RIVEL/RC -7,47").
   - Semantically match the text of the bonus line to the corresponding product(s) in the receipt. 
   - Calculate the total quantity of the matching products. Divide the absolute bonus amount by this total quantity to find the discount-per-unit.
   - For each matching product line, set the `discount` field to: `discount-per-unit * line_quantity` (as a positive float).
   - If an item receives no bonus, set `discount` to 0.

5. **Output**
   Return a JSON **array** where each element is an object with these **exact** keys:
   {
     "product_original": "...",
     "quantity": <number>,
     "price": <float>,         // unit price (PRIJS)
     "total_price": <float>,   // total price for this line (BEDRAG)
     "discount": <float>,      // total discount applied to this specific line (positive number)
     "date": "DD-MM-YYYY",
     "time": "HH:MM",
     "merchant": "..."
   }

Return ONLY the raw JSON array.
"""

LIDL_RECEIPT_PROMPT = """
You are a receipt parser for **Lidl** grocery receipts.
Extract every purchased item, plus metadata, and return a JSON array with the following fields:

- product_original: exact product name as printed.
- quantity: number of units (if shown as "2 x 1,19" then quantity=2, else default 1).
- price: unit price (e.g., from "2 x 1,19" the unit price is 1.19; from a single line like "2,49" price is that value).
- total_price: total price for this line (quantity * price).
- discount: total discount applied to this line (positive float, 0 if none). This is from a following "Lidl Plus korting" or "In prijs verlaagd" line with a negative amount.
- date: in DD-MM-YYYY format.
- time: in HH:MM format.
- merchant: the number after "Merchant" (e.g., "278508").

Rules:
- Parse the receipt sequentially. Each product line is usually a product name followed by a price (with comma as decimal separator). There may be a "B", "A", or "C" after the price - ignore these letters, they are VAT codes.
- If a line contains "x" (e.g., "2 x 1,19"), interpret as quantity = 2, price per unit = 1.19, total = 2.38.
- If a line contains a negative amount like "-0,70" immediately after a product line, that is a discount for that product. Apply it as a positive discount value to that product.
- Exclude lines that are not products: "Aantal", "Totaal", "Bankpas", "Kopie Kaarthouder", "Terminal", "Merchant", "AID", "Transactie", "DEBIT MASTERCARD", "Kaart", "Volgnr.", "Kaartbetaling", "geen CVM", "leesmethode", "Betaling gelukt", the VAT table, "Jouw totale besparing", etc.
- Date and time are near the bottom, e.g., "14-05-2026 14:52".
- Merchant: find the line with "Merchant" followed by a number.
- Output JSON array of objects with the exact keys listed.

Important: Use comma as decimal separator. Return only raw JSON, no markdown.
"""

GENERIC_RECEIPT_PROMPT = """
You are a receipt parser. Analyze this grocery receipt image and extract every purchased item.

Return ONLY a valid JSON array. No explanation, no markdown, no code fences.

Each element must have exactly these fields:
{
  "product_original": "exact product name as printed (in original language)",
  "quantity": <number, default 1 if not shown>,
  "price": <price per unit as float, e.g. 1.49>,
  "total_price": <same as price unless multiple quantity, then price*quantity>,
  "discount": 0,
  "date": "<receipt date as DD-MM-YYYY, or today if not visible>",
  "time": "<time as HH:MM if visible, else '00:00'>",
  "merchant": "<store identifier if visible, else empty>"
}

Rules:
- price must be the per-unit price.
- If a product has a discount shown, put it in discount as a positive number.
- date format must be DD-MM-YYYY.
- Do NOT include subtotals, totals, bags, deposit, or non-product lines.
"""

# ---------------------------------------------------------------------------
# GOOGLE SHEETS
# ---------------------------------------------------------------------------
def get_worksheet():
    gc = gspread.service_account(filename=CREDENTIALS_FILE)
    sh = gc.open_by_key(SHEET_ID)
    return sh.worksheet(SHEET_TAB)

def append_rows_to_sheet(rows: list[dict]):
    ws = get_worksheet()
    headers = ws.row_values(1)
    expected_headers = [
        "product_original", "product_english", "quantity", "price",
        "total_price", "discount", "date", "time", "merchant", "storeid", "id", "ids"
    ]
    if not headers:
        ws.append_row(expected_headers)
        headers = expected_headers
        
    for col in expected_headers:
        if col not in headers:
            headers.append(col)
            ws.update_cell(1, len(headers), col)

    formatted_rows = [[row.get(h, "") for h in headers] for row in rows]
    ws.append_rows(formatted_rows, value_input_option="USER_ENTERED")
    log.info(f"Appended {len(formatted_rows)} rows to sheet.")

# ---------------------------------------------------------------------------
# GEMINI VISION
# ---------------------------------------------------------------------------
def detect_store(image_bytes: bytes) -> str:
    client = genai.Client(api_key=GEMINI_API_KEY)
    response = client.models.generate_content(
        model="models/gemini-2.5-flash",
        contents=[
            "Analyze this receipt image. Reply with exactly one word: 'albert_heijn', 'lidl', or 'other'.",
            types.Part.from_bytes(data=image_bytes, mime_type="image/jpeg")
        ]
    )
    store = response.text.strip().lower()
    if store not in ["albert_heijn", "lidl"]:
        store = "other"
    return store

def parse_receipt_with_gemini(image_bytes: bytes) -> list[dict]:
    store = detect_store(image_bytes)
    log.info(f"Detected store: {store}")

    if store == "albert_heijn":
        prompt = AH_RECEIPT_PROMPT
    elif store == "lidl":
        prompt = LIDL_RECEIPT_PROMPT
    else:
        prompt = GENERIC_RECEIPT_PROMPT

    client = genai.Client(api_key=GEMINI_API_KEY)
    response = client.models.generate_content(
        model="models/gemini-2.5-flash",
        contents=[
            prompt,
            types.Part.from_bytes(data=image_bytes, mime_type="image/jpeg")
        ]
    )

    raw = response.text.strip()
    raw = re.sub(r"^```[a-z]*\n?", "", raw, flags=re.MULTILINE)
    raw = re.sub(r"\n?```$", "", raw, flags=re.MULTILINE)
    raw = raw.strip()

    items = json.loads(raw)
    if not isinstance(items, list):
        raise ValueError("Gemini did not return a JSON array.")
    return items

def normalize_items(items: list[dict]) -> list[dict]:
    today = datetime.now().strftime("%d-%m-%Y")
    normalized = []
    for item in items:
        store_raw = str(item.get("store", "")).lower().strip()
        
        if not store_raw:
            merchant = item.get("merchant", "")
            if "albert" in merchant.lower() or "ah" in merchant.lower() or len(merchant) == 8:
                store_raw = "albert heijn"
            elif "lidl" in merchant.lower() or len(merchant) == 6:
                store_raw = "lidl"
            else:
                store_raw = "unknown"
                
        storeid = next(
            (v for k, v in STORE_ID_MAP.items() if k in store_raw),
            store_raw.replace(" ", "_")
        )

        row = {
            "product_original": item.get("product_original", ""),
            "product_english":  "",
            "quantity":         item.get("quantity", 1),
            "price":            item.get("price", 0.0),
            "total_price":      item.get("total_price", item.get("price", 0.0) * item.get("quantity", 1)),
            "discount":         item.get("discount", 0.0),
            "date":             item.get("date") or today,
            "time":             item.get("time", ""),
            "merchant":         item.get("merchant", ""),
            "storeid":          storeid,
            "id":               "",
            "ids":              "",
        }
        normalized.append(row)
    return normalized

# ---------------------------------------------------------------------------
# TELEGRAM HANDLERS
# ---------------------------------------------------------------------------
async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "👋 Hi! I'm your grocery receipt bot.\n\n"
        "📸 Send me a photo of any grocery receipt and I'll log it to your sheet automatically.\n\n"
        "Supported stores: Albert Heijn, Lidl, Jumbo, Plus, Aldi, Dirk, and more.\n\n"
        "Commands:\n"
        "/start — show this message\n"
        "/status — check sheet connection"
    )

async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        ws = get_worksheet()
        count = len(ws.get_all_values()) - 1
        await update.message.reply_text(f"✅ Sheet connected. {count} purchases logged so far.")
    except Exception as e:
        await update.message.reply_text(f"❌ Sheet connection failed: {e}")

async def handle_photo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    await msg.reply_text("📸 Receipt received! Parsing with Gemini Vision...")

    try:
        photo = msg.photo[-1]
        file = await context.bot.get_file(photo.file_id)
        image_bytes = await file.download_as_bytearray()

        await msg.reply_text("🔍 Analyzing receipt...")

        items = parse_receipt_with_gemini(bytes(image_bytes))

        if not items:
            await msg.reply_text(
                "⚠️ Could not extract any products.\n"
                "Make sure the photo is clear and well-lit."
            )
            return

        rows = normalize_items(items)
        append_rows_to_sheet(rows)

        store = rows[0]["storeid"].replace("_", " ").title() if rows else "Unknown"
        date = rows[0]["date"] if rows else "?"
        time = rows[0]["time"] if rows and rows[0]["time"] else ""
        merchant = rows[0]["merchant"] if rows else ""
        
        summary = f"✅ Logged *{len(rows)} items* from *{store}*"
        if date:
            summary += f" on {date}"
        if time:
            summary += f" at {time}"
        if merchant:
            summary += f" (merchant {merchant})"
        summary += ":\n"
        
        for r in rows[:5]:
            price_str = f"€{float(r['price']):.2f}" if r['price'] else "?"
            total_str = f"€{float(r['total_price']):.2f}" if r['total_price'] else "?"
            disc_str = f" (disc. €{float(r['discount']):.2f})" if r['discount'] else ""
            summary += f"• {r['product_original']} × {r['quantity']} — {price_str} each, total {total_str}{disc_str}\n"
            
        if len(rows) > 5:
            summary += f"\n... and {len(rows)-5} more items."
            
        await msg.reply_text(summary, parse_mode="Markdown")

    except json.JSONDecodeError:
        await msg.reply_text("⚠️ Gemini returned an unexpected format. Try again with a clearer photo.")
    except Exception as e:
        log.error(f"Error processing receipt: {e}", exc_info=True)
        await msg.reply_text(f"❌ Error: {e}")

async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "📸 Please send a *photo* of your receipt.",
        parse_mode="Markdown"
    )

# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------
def main():
    log.info("Starting Grocery Receipt Bot...")

    missing = []
    if not TELEGRAM_TOKEN: missing.append("TELEGRAM_TOKEN")
    if not GEMINI_API_KEY: missing.append("GEMINI_API_KEY")
    if not SHEET_ID: missing.append("SHEET_ID")
    if missing:
        raise ValueError(f"Please set these environment variables: {missing}")

    app = ApplicationBuilder().token(TELEGRAM_TOKEN).build()
    app.add_handler(CommandHandler("start",  cmd_start))
    app.add_handler(CommandHandler("status", cmd_status))
    app.add_handler(MessageHandler(filters.PHOTO, handle_photo))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))

    log.info("Bot is running. Send /start in Telegram to test.")
    app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()