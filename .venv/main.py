from config import SEARCH_CONFIG
from scraper import scrape_main_results
from db import init_db
from verifier import verify_active_listings


def main():
    init_db()
    zip_code = SEARCH_CONFIG["zip"]
    radius = SEARCH_CONFIG["radius"]
    models = SEARCH_CONFIG["models"]
    seen_vins = set()

    all_makes = [entry["make"] for entry in models]
    all_models = [entry["model"] for entry in models]

    print(f"\n🔍 Starting scrape for: {', '.join(all_makes)}")

    # Local Search First
    local_vins = scrape_main_results(all_makes, all_models, "local", seen_vins, zip_code, radius)
    seen_vins.update(local_vins)

    # Then National Search, skipping VINs already seen locally
    national_vins = scrape_main_results(all_makes, all_models, "national", seen_vins, zip_code, radius)
    seen_vins.update(national_vins)

    print(f"\n✅ Finished scraping {len(seen_vins)} unique VINs")

    # Update the listings marked as active, and check on current status

    verify_active_listings()

if __name__ == "__main__":
    main()