from config import SEARCH_CONFIG, DB_PATH
from scraper import scrape_main_results
from db import init_db, refresh_cleaned_listings
from verifier import verify_active_listings
from status_tracker import StatusTracker
import time
import sqlite3
import pandas as pd
import page_fetcher

tracker = StatusTracker()


def main():
    start_time = time.time()
    init_db()
    zip_code = SEARCH_CONFIG["zip"]
    radius = SEARCH_CONFIG["radius"]
    models = SEARCH_CONFIG["models"]
    seen_vins = set()
    new_vins_by_model = {}
    updated_vins_by_model = {}

    for entry in SEARCH_CONFIG["models"]:
        tracker.register_model(entry["make"], entry["model"])

    tracker.start_refresh_loop()

    print(f"\n Starting scrape by model:")

    for entry in models:
        model_start_time = time.time()
        make = entry["make"]
        model = entry["model"]

        local_vins, local_updated = scrape_main_results(
            [make], [model], "local", seen_vins, zip_code, radius)
        seen_vins.update(local_vins)
        print()  # newline after local
        print(f" - Total Requests: {page_fetcher.total_requests_made}")
        print(f" - Total Downloaded: {page_fetcher.total_bytes_downloaded / 1024 / 1024:.2f} MB")

        # Scrape national with timer feedback
        national_vins, national_updated = scrape_main_results([make], [model], "national", seen_vins, zip_code, radius)

        total_new = len(local_vins) + len(national_vins)
        total_updated = local_updated + national_updated
        new_vins_by_model[f"{make} {model}"] = total_new
        updated_vins_by_model[f"{make} {model}"] = total_updated

        print(f"\n Finished scraping {model}, {len(seen_vins)} unique VINs, {total_new} new VINs, {total_updated} VINs updated.")
        elapsed = time.time() - model_start_time
        mins, secs = divmod(int(elapsed), 60)
        print(f" Model run time: {mins}m {secs}s")
        # Data usage
        print(f"\n 📡 Estimated Data Usage:")
        print(f" - Total Requests: {page_fetcher.total_requests_made}")
        print(f" - Total Downloaded: {page_fetcher.total_bytes_downloaded / 1024 / 1024:.2f} MB")

    refresh_cleaned_listings()
    verify_active_listings()

    tracker.stop()


if __name__ == "__main__":
    main()
