from config import SEARCH_CONFIG
from scraper import scrape_main_results
from db import init_db, refresh_cleaned_listings
from verifier import verify_active_listings
import subprocess


def main():
    init_db()
    zip_code = SEARCH_CONFIG["zip"]
    radius = SEARCH_CONFIG["radius"]
    models = SEARCH_CONFIG["models"]
    seen_vins = set()

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

    print(f"\n Finished scraping {len(seen_vins)} unique VINs")

    # Update the listings marked as active, and check on current status

    refresh_cleaned_listings()

    verify_active_listings()



if __name__ == "__main__":
    main()