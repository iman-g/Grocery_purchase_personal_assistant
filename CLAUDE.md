# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A personal grocery tracker for Dutch supermarkets. It has two independent data paths that both feed a single Google Sheet (the "Raw" tab), which a Streamlit dashboard then visualizes:

1. **Automated daily scraping** (`run.py`, run by GitHub Actions cron) — scrapes Albert Heijn and Lidl catalogs/offers, translates product names NL→EN, and fuzzy-matches existing purchase rows in the Sheet to AH product IDs. This produces "what's on offer" data, not purchase history.
2. **Manual receipt logging** (`telegram_bot.py`) — user sends a receipt photo to a Telegram bot, Gemini Vision extracts line items, and they're appended directly to the Sheet as actual purchase history.

`app.py` is the Streamlit dashboard: it reads the Sheet, shows spending KPIs/charts, and has a basic RAG chat tab (dumps recent rows into a Gemini prompt).

## Commands

No build step, no test suite, no linter is configured in this repo — it's plain scripts.

```
python -m venv venv && venv\Scripts\activate      # Windows
pip install -r requirements.txt

python run.py                # full daily pipeline: lidl -> albert_heijn -> file_trans -> map_purchases
python albert_heijn.py       # scrape AH only -> ah_full_export1_*.csv, ah_summary1_*.csv
python lidl.py                # scrape Lidl only -> lidl_offers_*.csv
python map_purchases.py      # fuzzy-match unmapped AH purchase rows in the Google Sheet to product IDs
streamlit run app.py         # dashboard + AI chat
python telegram_bot.py       # long-running Telegram bot (receipt photo -> Sheet)
```

`file_trans.py` is not meant to be run directly (`python file_trans.py` just prints a message) — it's called from `run.py` with filenames produced by the scrapers that run.

## Required local credentials (not in the repo)

- `grocery_tracker.json` — Google service account key, gitignored. Required by `map_purchases.py` and `telegram_bot.py` (via `gspread`). In CI it's written from the `GCP_JSON_KEY` secret (see `.github/workflows/daily_pipeline.yml`).
- `.streamlit/secrets.toml` with `[gemini] api_key = "..."` and gsheets connection config — required by `app.py`. Not present in this checkout.
- `telegram_bot.py` currently reads `TELEGRAM_TOKEN`, `GEMINI_API_KEY`, `SHEET_ID` via `os.getenv(...)` **with live credentials hardcoded as the fallback default**. If you touch this file, move those to env-only before anything gets committed — don't let the hardcoded values get pushed.

## Architecture notes

- **Google Sheet "Raw" tab is the single source of truth for purchases.** Columns: `product_original, product_english, quantity, price, discount, date, storeid, id, ids`. `id`/`ids` are AH product IDs filled in by `map_purchases.py`; only rows where `store` contains `albert_heijn` get matched — other stores are logged but never get an `id`.
- **`product_translation_memory.csv`** is a persistent cache keyed by AH product `id` (`id, dutch_title, english_title`) so `deep_translator` calls only happen once per product ever. It's also the database `map_purchases.py` fuzzy-matches against (via `rapidfuzz`, threshold 85). Both `file_trans.py` and `map_purchases.py` read/write this file, and `file_trans.py` deduplicates it on every load/save — don't bypass that when editing it directly.
- **`map_purchases.py` matching order**: for each unmapped AH row, it first checks the Sheet's own history for an exact `product_original` match (fast path, "learned" mappings), and only falls back to `rapidfuzz` fuzzy matching against the translation memory if there's no exact history hit.
- **Albert Heijn scraping** (`albert_heijn.py`) uses AH's authenticated mobile API (`api.ah.nl/mobile-services/product/search/v2`), not the public website — it fetches an anonymous OAuth token first (`get_access_token`) and refreshes on 401. Price/discount parsing (`parse_price_and_discount`) has to handle three distinct AH bonus structures (simple price cut, multi-item deals like "2 voor €0.89", and no discount) — read the docstring on that function before changing pricing logic, the AH API's fields (`currentPrice`, `priceBeforeBonus`, `discountLabels`, `bonusMechanism`) are not self-explanatory.
- **Lidl scraping** (`lidl.py`) has no API — it pulls embedded JSON out of `data-grid-data` HTML attributes on the offers page and has separate logic for "Lidl Plus" member pricing vs. standard pricing.
- **Two different Gemini SDKs are in use**: `app.py` uses the older `google.generativeai`, while `telegram_bot.py` uses the newer `google.genai`. Don't assume API shape carries over between them.
- **Daily CSV outputs get committed by CI itself**, not by you: `.github/workflows/daily_pipeline.yml` runs `run.py`, moves the dated CSVs into `stores/`, and pushes a `🤖 Daily Data Update` commit as "GitHub Actions Bot". Manual commits to this repo are rare compared to bot commits — check commit authorship, not just recency, when looking at history.

## Entry points

| File | How it's run | Purpose |
|---|---|---|
| `run.py` | `python run.py`; daily via GitHub Actions cron (`0 15 * * *` UTC) | Orchestrates: scrape Lidl → scrape AH → translate → map purchases. Each step wrapped in try/except so one failure doesn't kill the rest. |
| `app.py` | `streamlit run app.py` | Dashboard + AI chat, reads the Google Sheet via `streamlit_gsheets`, cached 60s. |
| `telegram_bot.py` | `python telegram_bot.py` (long-running poller) | Telegram bot: receipt photo → Gemini Vision → appended to the Sheet. |

## Data model reference

**Google Sheet "Raw" tab** — canonical purchase record:
`product_original, product_english, quantity, price, discount, date, storeid, id, ids`

**`stores/ah_full_export1_*.csv`** — daily AH catalog scrape, one row per product:
`id, hq_id, title, brand, scraped_aisle, main_category, sub_category, final_price, original_price, discount_pct, discount_label, deal_price, deal_count, unit, unit_price_description, is_bonus, discount_type, bonus_mechanism, bonus_start, bonus_end, nutriscore, available_online, shop_type, url, scraped_at`

**`stores/ah_summary1_*.csv`** — per-aisle rollup: `scraped_aisle, items_found, bonus_items`.

**`stores/lidl_offers_*.csv`** — Lidl offers scrape: `title, price, old_price, discount_percent, discount_label, deal_type, unit, url, scraped_at`.

**`product_translation_memory.csv`** — `id, dutch_title, english_title` (see cache note above).

**`*_translated.csv`** — source CSV plus `title_eng` / `aisle_eng` columns, produced by `file_trans.py`.

## Current project state (as of 2026-07-08)

The project was paused for ~5 months. No TODO/FIXME/stub comments exist anywhere in the code — what's here instead is real, largely-finished work that was never committed:

- **Git history**: the last human commit was `a33bf11` (2026-02-13, "Update Python version in daily pipeline to 3.11"). Everything after that through `de95fc6` (2026-02-24) is the automated daily-scrape bot committing CSVs — the cron job kept running after manual work stopped.
- **`albert_heijn.py` has an uncommitted rewrite** (~342-line diff): migrates the scraper from AH's old public search endpoint (`www.ah.nl/zoeken/api/products/search`, now broken/blocked) to their authenticated mobile API (`api.ah.nl/mobile-services/product/search/v2`), adding OAuth token handling and the three-case bonus/discount parsing described above. Functionally complete-looking but untested/uncommitted.
- **`telegram_bot.py` is new and untracked** — a fully-built receipt-photo bot (`/start`, `/status`, photo handler, text fallback). Built right after a scratch script (`test.py`, also untracked) was used to look up the right Gemini model name. This was the last thing actively worked on. It still has the hardcoded-credentials issue noted above.
- **`requirements.txt` has one uncommitted line added** (`python-telegram-bot>=20.0`) to support the bot.
- **Unresolved since the original Feb 12 upload**: `map_purchases.py` (~line 97) has a self-doubting comment on the row-selection condition — `# NOTE: I assumed you want 'in' AH. If you meant 'not in', change this back!` — worth confirming the intended logic before relying on it.

This section is a point-in-time snapshot, not a durable fact — re-check `git status` and `git log` rather than trusting it once the working tree has moved on.
