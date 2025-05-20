from config import SEARCH_CONFIG
from scraper import scrape_main_results
from db import init_db, refresh_cleaned_listings
from verifier import verify_active_listings
import time
import sqlite3
import pandas as pd
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "data", "cars.db")

def main():
    start_time = time.time()
    init_db()
    zip_code = SEARCH_CONFIG["zip"]
    radius = SEARCH_CONFIG["radius"]
    models = SEARCH_CONFIG["models"]
    seen_vins = set()
    new_vins_by_model = {}

    print(f"\n Starting scrape by model:")

    for entry in models:
        make = entry["make"]
        model = entry["model"]

        print(f"\n🔍 Scraping: {make} {model} (local)")
        local_vins = scrape_main_results([make], [model], "local", seen_vins, zip_code, radius)
        seen_vins.update(local_vins)

        print(f"🔍 Scraping: {make} {model} (national)")
        national_vins = scrape_main_results([make], [model], "national", seen_vins, zip_code, radius)
        seen_vins.update(national_vins)

        total_new = len(local_vins) + len(national_vins)
        new_vins_by_model[f"{make} {model}"] = total_new

    print(f"\n Finished scraping {len(seen_vins)} unique VINs")

    refresh_cleaned_listings()
    verify_active_listings()

    # Summary output
    print("\n📊 Scrape Summary:")
    for model, count in new_vins_by_model.items():
        print(f" - {model}: {count} new VINs")

    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query("SELECT COUNT(*) as count FROM listings WHERE status = 'active'", conn)
    conn.close()
    print(f"\n✅ Total active VINs in DB: {df['count'].iloc[0]}")

    elapsed = time.time() - start_time
    mins, secs = divmod(int(elapsed), 60)
    print(f"⏱️ Total run time: {mins}m {secs}s")

if __name__ == "__main__":
    main()
