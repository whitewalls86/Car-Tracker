import time
import random
from datetime import date, timedelta
from urllib.parse import urlencode
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from config import SEARCH_CONFIG
from db import listing_exists_by_listing_id, listing_exists_by_vin, save_or_update_listing, log_price

# Setup Selenium driver
chrome_options = Options()
chrome_options.add_argument("--headless")
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("--window-size=1920x1080")
driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)

BASE_URL = "https://www.cars.com/shopping/results/"
SHIPPING_COST_PER_MILE = SEARCH_CONFIG["shipping_cost_per_mile"]
PAGE_SIZE = SEARCH_CONFIG.get("page_size", 20)
MAX_PAGES = SEARCH_CONFIG.get("pages", 5)


def scrape_main_results(makes, models, scope, seen_vins, zip_code, radius):
    seen_today = []

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
        print(f"\U0001f310 [{scope.upper()}] Page {page_num}: {full_url}")

        try:
            driver.get(full_url)
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "div.vehicle-card"))
            )
            soup = BeautifulSoup(driver.page_source, "html.parser")
            cards = soup.select("div.vehicle-card")
            print(f"\U0001f4e6 Found {len(cards)} vehicle cards")

            for i, card in enumerate(cards):
                try:
                    listing_id = card.get("data-listing-id")
                    print(f"\n🔎 Processing listing {i + 1}/{len(cards)} — ID: {listing_id}")

                    if listing_exists_by_listing_id(listing_id):
                        print("   ↪ Already seen this listing_id, skipping detail scrape.")
                        continue

                    relative_url = card.select_one("a.image-gallery-link")["href"]
                    detail_url = f"https://www.cars.com{relative_url}"
                    detail_data = scrape_detail_page(detail_url)
                    vin = detail_data.get("vin")
                    if not vin or vin in seen_vins:
                        print("   ↪ VIN not found or already processed.")
                        continue

                    title = card.select_one("h2.title").text.strip()
                    price_el = card.select_one("span.primary-price")
                    price = int(price_el.text.strip().replace("$", "").replace(",", "")) if price_el and "$" in price_el.text else None
                    msrp_el = card.select_one("span.secondary-price")
                    if msrp_el and "MSRP" in msrp_el.text:
                        msrp = int(msrp_el.text.strip().replace("MSRP", "").replace("$", "").replace(",", "").strip())
                    else:
                        msrp = None
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

                    save_or_update_listing(listing)
                    log_price(vin, price)
                    print(f"   ✅ Saved {vin} | ${price} | {dealer}")
                    seen_today.append(vin)

                except Exception as e:
                    print(f"⚠️ Error parsing card: {e}")
        except Exception as e:
            print(f"❌ Error on page {page_num}: {e}")

    return seen_today


def scrape_detail_page(url):
    try:
        print(f"   ↪ Loading detail page: {url}")
        time.sleep(random.uniform(2.0, 4.0))
        driver.get(url)
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "div.vin"))
        )
        soup = BeautifulSoup(driver.page_source, "html.parser")

        vin_input = soup.find("input", {"aria-label": "VIN (optional)"})
        vin = vin_input["value"].strip() if vin_input else None

        listed_time_tag = soup.select_one('div.price-history-summary div.listed-time strong')
        days_on_market = int(listed_time_tag.text.strip()) if listed_time_tag else None

        mileage_and_vin = extract_mileage_and_vin(soup)
        mileage = mileage_and_vin[0]
        vin = mileage_and_vin[1]

        print(f"   ↪ Extracted VIN: {vin}, Days on market: {days_on_market}, Mileage: {mileage}")
        return {"vin": vin, "days_on_market": days_on_market, "mileage": mileage}

    except Exception as e:
        print(f"[detail scrape error] {url} — {e}")
        return {"vin": None, "days_on_market": None, "mileage": None}


def extract_mileage_and_vin(soup):
    found_vin = False
    found_mileage = False
    dt_tags = soup.find_all("dt")
    for dt in dt_tags:
        if dt.text.strip().lower() == "mileage":
            dd = dt.find_next_sibling("dd")
            if dd:
                mileage_text = dd.text.strip().replace(" mi.", "").replace(",", "")
                if mileage_text.isdigit():
                    found_mileage = True
        if dt.text.strip().lower() == "vin":
            dd = dt.find_next_sibling("dd")
            if dd:
                vin_text = dd.text.strip()
                found_vin = True

        if found_vin and found_mileage:
            return int(mileage_text), vin_text

    return None
