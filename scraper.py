import time
import random
from datetime import date, timedelta
from urllib.parse import urlencode
from concurrent.futures import ThreadPoolExecutor, as_completed
from queue import Queue
import threading
from config import SEARCH_CONFIG
from db import listing_exists_by_listing_id, update_and_log, get_vin_from_listing_id
from page_fetcher import fetch_soup_with_fallback
from collections import deque, Counter


BASE_URL = "https://www.cars.com/shopping/results/"
SHIPPING_COST_PER_MILE = SEARCH_CONFIG["shipping_cost_per_mile"]
PAGE_SIZE = SEARCH_CONFIG.get("page_size", 20)
MAX_PAGES = SEARCH_CONFIG.get("pages", 5)

db_queue = Queue()
seen_vins_lock = threading.Lock()
recent_request_types = deque(maxlen=100)


def db_writer():
    while True:
        item = db_queue.get()
        if item is None:
            db_queue.task_done()
            break
        try:
            listing, price = item
            update_and_log(listing)
        except Exception as e:
            print(f"\n DB write error: {e}")
        finally:
            db_queue.task_done()


def scrape_detail_page(url):
    try:
        time.sleep(random.uniform(2.0, 4.0))
        soup, req_type = fetch_soup_with_fallback(url, 10)
        if not soup:
            return {"vin": None, "days_on_market": None, "mileage": None, "request_type": None}

        listed_time_tag = soup.select_one('div.price-history-summary div.listed-time strong')
        days_on_market = int(listed_time_tag.text.strip()) if listed_time_tag else None

        mileage, vin = extract_mileage_and_vin(soup)
        return {"vin": vin, "days_on_market": days_on_market, "mileage": mileage, "request_type": req_type}
    except Exception as e:
        print(f"[detail scrape error] {url} — {e}")
        return {"vin": None, "days_on_market": None, "mileage": None, "request_type": None}


def extract_mileage_and_vin(soup):
    mileage = None
    vin = None
    dt_tags = soup.find_all("dt")
    for dt in dt_tags:
        if dt.text.strip().lower() == "mileage":
            dd = dt.find_next_sibling("dd")
            if dd:
                mileage_text = dd.text.strip().replace(" mi.", "").replace(",", "")
                if mileage_text.isdigit():
                    mileage = int(mileage_text)
        if dt.text.strip().lower() == "vin":
            dd = dt.find_next_sibling("dd")
            if dd:
                vin = dd.text.strip()
    return mileage, vin


def process_card(card, scope, seen_vins):
    try:
        listing_id = card.get("data-listing-id")
        if listing_exists_by_listing_id(listing_id):
            # Skip detail page — grab VIN from DB
            row = get_vin_from_listing_id(listing_id)

            if row:
                try:
                    vin = row[0]
                    price_el = card.select_one("span.primary-price")
                    price = int(price_el.text.strip().replace("$", "").replace(",",
                                                                               "")) if price_el and "$" in price_el.text else None
                    listing = {
                        "vin": vin,
                        "listing_id": listing_id,
                        "title": None,
                        "price": price,
                        "mileage": None,
                        "dealer": None,
                        "location": None,
                        "distance": None,
                        "shipping_cost": None,
                        "search_scope": scope,
                        "url": None,
                        "image_url": None,
                        "days_on_market": None,
                        "date_added": None,
                        "msrp": None
                    }
                    db_queue.put((listing, price))
                    return vin, "cached", True
                except Exception as e:
                    print(f"[card scrape error] {e}")
            else:
                return None, None, False

        relative_url = card.select_one("a.image-gallery-link")["href"]
        detail_url = f"https://www.cars.com{relative_url}"
        detail_data = scrape_detail_page(detail_url)
        vin = detail_data.get("vin")
        if not vin:
            return None, None

        with seen_vins_lock:
            if vin in seen_vins:
                return None, None
            seen_vins.add(vin)

        title = card.select_one("h2.title").text.strip()
        price_el = card.select_one("span.primary-price")
        price = int(price_el.text.strip().replace("$", "").replace(",", "")) if price_el and "$" in price_el.text else None
        msrp_el = card.select_one("span.secondary-price")
        msrp = int(msrp_el.text.strip().replace("MSRP", "").replace("$", "").replace(",", "").strip()) if msrp_el and "MSRP" in msrp_el.text else None
        dealer = card.select_one("div.dealer-name strong").text.strip()
        location = card.select_one("div.miles-from").text.strip()
        img_tag = card.select_one("img.vehicle-image")
        image_url = img_tag["src"] if img_tag else None

        try:
            raw_distance = location.split("(")[-1].split("mi.")[0].strip().replace(",", "")
            distance = int(raw_distance)
        except (ValueError, TypeError):
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
        return vin, detail_data.get("request_type"), True  # Indicates "updated"

    except Exception as e:
        print(f"\n Error parsing card: {e}")
        return None, None, False


def scrape_main_results(makes, models, scope, seen_vins, zip_code, radius):
    seen_today = []
    updated_vins_count = 0
    start_time = time.time()
    db_thread = threading.Thread(target=db_writer, daemon=True)
    db_thread.start()

    card_queue = Queue()

    def page_loader(page_num):
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
        elapsed = int(time.time() - start_time)
        print(f"\r {models}-{scope} | Page {page_num} | Processed VINs: {len(seen_today)} | Card Queue: {card_queue.qsize()} | DB Queue: {db_queue.qsize()} | Elapsed: {elapsed}s", end="", flush=True)
        soup, request_type = fetch_soup_with_fallback(full_url, 10)
        if not soup:
            print(f"\n Error on page {page_num}: unable to fetch page.")
            return
        cards = soup.select("div.vehicle-card")
        for card in cards:
            card_queue.put(card)

    def card_worker():
        while True:
            card = card_queue.get()
            if card is None:
                card_queue.task_done()
                break
            vin, request_type, was_updated = process_card(card, scope, seen_vins)
            if was_updated:
                nonlocal updated_vins_count
                updated_vins_count += 1
            if request_type:
                recent_request_types.append(request_type)
            if vin:
                seen_today.append(vin)
            method_counts = Counter(recent_request_types)
            summary = " | ".join(f"{m}: {c}%" for m, c in method_counts.items())
            elapsed = int(time.time() - start_time)
            elapsed_min, elapsed_sec = divmod(elapsed, 60)
            print(f"\r {models}-{scope} | Processed VINs: {len(seen_today)} | Card Queue: {card_queue.qsize()} | DB Queue: {db_queue.qsize()} | Elapsed: {elapsed_min}m {elapsed_sec}s | {summary}", end="", flush=True)
            card_queue.task_done()

    # Start page loader threads
    with ThreadPoolExecutor(max_workers=20) as page_executor:
        futures = [page_executor.submit(page_loader, i) for i in range(1, MAX_PAGES + 1)]
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                print(f"\n Error loading a page: {e}")

    # Start card processor threads
    workers = []
    for _ in range(21):
        t = threading.Thread(target=card_worker)
        t.start()
        workers.append(t)

    card_queue.join()
    for _ in workers:
        card_queue.put(None)
    for t in workers:
        t.join()

    db_queue.join()
    db_queue.put(None)
    db_thread.join()
    print()
    return seen_today, updated_vins_count
