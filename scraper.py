# Updated scraper.py and verifier.py with multithreading
import time
import random
from datetime import date, timedelta, datetime
from urllib.parse import urlencode
from bs4 import BeautifulSoup
import requests
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from queue import Queue
from config import SEARCH_CONFIG
from db import listing_exists_by_listing_id, listing_exists_by_vin, save_or_update_listing, log_price

BASE_URL = "https://www.cars.com/shopping/results/"
SHIPPING_COST_PER_MILE = SEARCH_CONFIG["shipping_cost_per_mile"]
PAGE_SIZE = SEARCH_CONFIG.get("page_size", 20)
MAX_PAGES = SEARCH_CONFIG.get("pages", 5)

seen_vins_lock = threading.Lock()
db_queue = Queue()


def db_writer():
    while True:
        item = db_queue.get()
        if item is None:
            break
        listing, price = item
        save_or_update_listing(listing)
        log_price(listing["vin"], price)
        db_queue.task_done()


def fetch_page(url):
    try:
        res = requests.get(url, timeout=10)
        if res.status_code == 200:
            return BeautifulSoup(res.text, "html.parser")
    except:
        return None


def scrape_detail_page(url):
    try:
        time.sleep(random.uniform(1.5, 3.5))
        soup = fetch_page(url)
        if not soup:
            return {"vin": None, "days_on_market": None, "mileage": None}

        vin_tag = soup.select_one("input[aria-label='VIN (optional)']")
        vin = vin_tag["value"].strip() if vin_tag else None
        listed_time_tag = soup.select_one('div.price-history-summary div.listed-time strong')
        days_on_market = int(listed_time_tag.text.strip()) if listed_time_tag else None
        mileage = None
        mileage_tag = soup.select_one("dt:contains('Mileage') + dd")
        if mileage_tag:
            mileage = int(mileage_tag.text.strip().replace(" mi.", "").replace(",", ""))

        return {"vin": vin, "days_on_market": days_on_market, "mileage": mileage}
    except:
        return {"vin": None, "days_on_market": None, "mileage": None}


def process_card(card, models, scope, seen_vins, start_time):
    try:
        listing_id = card.get("data-listing-id")
        if listing_exists_by_listing_id(listing_id):
            return None

        relative_url = card.select_one("a.image-gallery-link")["href"]
        detail_url = f"https://www.cars.com{relative_url}"
        detail_data = scrape_detail_page(detail_url)
        vin = detail_data.get("vin")
        if not vin:
            return None

        with seen_vins_lock:
            if vin in seen_vins:
                return None
            seen_vins.add(vin)

        title = card.select_one("h2.title").text.strip()
        price_el = card.select_one("span.primary-price")
        price = int(price_el.text.strip().replace("$", "").replace(",", "")) if price_el and "$" in price_el.text else None
        msrp_el = card.select_one("span.secondary-price")
        msrp = int(msrp_el.text.strip().replace("MSRP", "").replace("$", "").replace(",", "").strip()) if msrp_el else None
        dealer = card.select_one("div.dealer-name strong").text.strip()
        location = card.select_one("div.miles-from").text.strip()
        img_tag = card.select_one("img.vehicle-image")
        image_url = img_tag["src"] if img_tag else None

        try:
            raw_distance = location.split("(")[-1].split("mi.")[0].strip().replace(",", "")
            distance = int(raw_distance)
        except:
            distance = None

        shipping_cost = round(distance * SHIPPING_COST_PER_MILE, 2) if distance else None

        listing = {
            "vin": vin,
            "listing_id": listing_id,
            "title": title,
            "price": price,
            "mileage": detail_data.get("mileage"),
            "dealer": dealer,
            "location": location,
            "distance": distance,
            "shipping_cost": shipping_cost,
            "search_scope": scope,
            "url": detail_url,
            "image_url": image_url,
            "days_on_market": detail_data.get("days_on_market"),
            "date_added": (date.today() - timedelta(days=detail_data.get("days_on_market", 0))) if detail_data.get("days_on_market") else None,
            "msrp": msrp
        }

        db_queue.put((listing, price))
        return vin

    except Exception as e:
        print(f"Error parsing card: {e}")
        return None


def scrape_main_results(makes, models, scope, seen_vins, zip_code, radius):
    seen_today = []
    start_time = time.time()

    db_thread = threading.Thread(target=db_writer, daemon=True)
    db_thread.start()

    for page_num in range(1, MAX_PAGES + 1):
        params = {
            "makes[]": makes,
            "models[]": models,
            "stock_type": "new",
            "zip": zip_code,
            "page": page_num,
            "page_size": PAGE_SIZE,
            "maximum_distance": radius if scope == "local" else "all"
        }

        full_url = BASE_URL + "?" + urlencode(params, doseq=True)
        soup = fetch_page(full_url)
        if not soup:
            print(f"\n Error on page {page_num}: unable to fetch page.")
            continue

        cards = soup.select("div.vehicle-card")

        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(process_card, card, models, scope, seen_vins, start_time) for card in cards]
            for i, future in enumerate(as_completed(futures), 1):
                vin = future.result()
                if vin:
                    seen_today.append(vin)
                elapsed = int(time.time() - start_time)
                print(f"\r🔄 {models}-{scope} page {page_num}/{MAX_PAGES} | {i}/{len(cards)} cards | Elapsed: {elapsed}s", end="", flush=True)

    db_queue.join()
    db_queue.put(None)  # Stop the db_writer
    db_thread.join()

    print()
    return seen_today