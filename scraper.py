# scraper.py with full original features + multithreading
import time
import random
import os
from datetime import date, timedelta
from urllib.parse import urlencode
from bs4 import BeautifulSoup
import requests
import cloudscraper
from fake_useragent import UserAgent
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from concurrent.futures import ThreadPoolExecutor, as_completed
from queue import Queue
import threading
from config import SEARCH_CONFIG
from db import listing_exists_by_listing_id, save_or_update_listing, log_price

BASE_URL = "https://www.cars.com/shopping/results/"
SHIPPING_COST_PER_MILE = SEARCH_CONFIG["shipping_cost_per_mile"]
PAGE_SIZE = SEARCH_CONFIG.get("page_size", 20)
MAX_PAGES = SEARCH_CONFIG.get("pages", 5)

VALID_UA_LOG = "valid_user_agents.txt"
valid_user_agents = []
if os.path.exists(VALID_UA_LOG):
    with open(VALID_UA_LOG, "r", encoding="utf-8") as f:
        valid_user_agents = [line.strip() for line in f if line.strip()]

ua = UserAgent()
db_queue = Queue()
seen_vins_lock = threading.Lock()


def db_writer():
    while True:
        item = db_queue.get()
        if item is None:
            db_queue.task_done()
            break
        try:
            listing, price = item
            save_or_update_listing(listing)
            log_price(listing["vin"], price)
        except Exception as e:
            print(f"\n DB write error: {e}")
        finally:
            db_queue.task_done()


def fetch_page_with_fallback(url, wait_for_selector="div.vehicle-card"):
    user_agents_to_try = valid_user_agents.copy() or [ua.random for _ in range(3)]
    for ua_string in user_agents_to_try:
        try:
            headers = {"User-Agent": ua_string}
            res = requests.get(url, headers=headers, timeout=5)
            if res.status_code == 200:
                return BeautifulSoup(res.text, "html.parser")
        except:
            continue

    try:
        scraper = cloudscraper.create_scraper()
        res = scraper.get(url, timeout=10)
        if res.status_code == 200:
            return BeautifulSoup(res.text, "html.parser")
    except:
        pass

    try:
        options = Options()
        options.add_argument("--headless")
        options.add_argument("--disable-gpu")
        options.add_argument("--window-size=1920x1080")
        service = Service(executable_path=ChromeDriverManager().install(), log_path="/dev/null")  # Use "NUL" on Windows
        driver = webdriver.Chrome(service=service, options=options)
        driver.get(url)
        if wait_for_selector:
            WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CSS_SELECTOR, wait_for_selector)))
        html = driver.page_source
        driver.quit()
        return BeautifulSoup(html, "html.parser")
    except Exception as e:
        print(f"[Selenium fallback error] {url} — {e}")
        return None


def scrape_detail_page(url):
    try:
        time.sleep(random.uniform(2.0, 4.0))
        soup = fetch_page_with_fallback(url, wait_for_selector="div.vin")
        if not soup:
            return {"vin": None, "days_on_market": None, "mileage": None}

        vin_input = soup.find("input", {"aria-label": "VIN (optional)"})
        vin = vin_input["value"].strip() if vin_input else None

        listed_time_tag = soup.select_one('div.price-history-summary div.listed-time strong')
        days_on_market = int(listed_time_tag.text.strip()) if listed_time_tag else None

        mileage, vin = extract_mileage_and_vin(soup)
        return {"vin": vin, "days_on_market": days_on_market, "mileage": mileage}
    except Exception as e:
        print(f"[detail scrape error] {url} — {e}")
        return {"vin": None, "days_on_market": None, "mileage": None}


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
        msrp = int(msrp_el.text.strip().replace("MSRP", "").replace("$", "").replace(",", "").strip()) if msrp_el and "MSRP" in msrp_el.text else None
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
        print(f"\n Error parsing card: {e}")
        return None


def scrape_main_results(makes, models, scope, seen_vins, zip_code, radius):
    seen_today = []
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
        print(f"\r {models}-{scope} | Page {page_num} | Processed VINs: {len(seen_today)} | Queue: {card_queue.qsize()} | Elapsed: {elapsed}s", end="", flush=True)
        soup = fetch_page_with_fallback(full_url, wait_for_selector="div.vehicle-card")
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
            vin = process_card(card, models, scope, seen_vins, start_time)
            if vin:
                seen_today.append(vin)
            elapsed = int(time.time() - start_time)
            print(f"\r {models}-{scope} | Processed VINs: {len(seen_today)} | Queue: {card_queue.qsize()} | Elapsed: {elapsed}s", end="", flush=True)
            card_queue.task_done()

    # Start page loader threads
    with ThreadPoolExecutor(max_workers=5) as page_executor:
        futures = [page_executor.submit(page_loader, i) for i in range(1, MAX_PAGES + 1)]
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                print(f"\n Error loading a page: {e}")

    # Start card processor threads
    workers = []
    for _ in range(10):
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
    return seen_today
