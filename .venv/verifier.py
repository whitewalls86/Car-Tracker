import sqlite3
from datetime import date, datetime
from bs4 import BeautifulSoup
import requests
from fake_useragent import UserAgent
import time
import random
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
import cloudscraper
import os


failed_user_agents = set()
used_user_agents = set()
valid_user_agents = set()
FAILED_UA_LOG = "failed_user_agents.log"
SUCCESS_UA_LOG = "successful_user_agents.log"
VALID_UA_LOG = "valid_user_agents.txt"

def check_listing_still_active(soup):
    unlisted_notice = soup.select_one("spark-notification.unlisted-notification[open]")
    return unlisted_notice is None

def extract_price(soup):
    price_el = soup.select_one("span.primary-price")
    if price_el and "$" in price_el.text:
        return int(price_el.text.strip().replace("$", "").replace(",", ""))
    return None

def fetch_with_selenium(url):
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920x1080")
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    driver.get(url)
    time.sleep(3)
    html = driver.page_source
    driver.quit()
    return BeautifulSoup(html, "html.parser")

def log_user_agent(ua_string, log_file):
    with open(log_file, "a", encoding="utf-8") as f:
        f.write(ua_string + "\n")

def fetch_with_retries(url, user_agent, max_retries=3):
    for attempt in range(max_retries):
        if valid_user_agents:
            ua_string = random.choice(list(valid_user_agents))
        else:
            ua_string = user_agent.random
        headers = {"User-Agent": ua_string}
        try:
            res = requests.get(url, headers=headers, timeout=5)
            used_user_agents.add(ua_string)
            log_user_agent(ua_string, SUCCESS_UA_LOG)
            return BeautifulSoup(res.text, "html.parser")
        except requests.exceptions.RequestException:
            failed_user_agents.add(ua_string)
            log_user_agent(ua_string, FAILED_UA_LOG)
            if attempt < max_retries - 1:
                print(f"⏱️ Retry {attempt + 1} for {url} with new User-Agent")
                time.sleep(random.uniform(0.5, 1.0))
            else:
                print(f"⏱️ Requests failed for {url}, trying cloudscraper")
                try:
                    scraper = cloudscraper.create_scraper()
                    res = scraper.get(url, timeout=10)
                    return BeautifulSoup(res.text, "html.parser")
                except Exception as cs_e:
                    print(f"❌ cloudscraper failed for {url}: {cs_e}")
                    return None

def verify_active_listings():
    today = date.today()
    conn = sqlite3.connect("data/cars.db")
    cur = conn.cursor()

    cur.execute("""
        SELECT vin, url FROM listings
        WHERE status = 'active' AND last_seen < ?
    """, (today,))
    rows = cur.fetchall()

    print(f"🔎 Verifying {len(rows)} listings...")

    ua = UserAgent()
    retry_list = []
    start_time = datetime.now()

    # Load known valid UAs from file
    if os.path.exists(VALID_UA_LOG):
        with open(VALID_UA_LOG, "r", encoding="utf-8") as f:
            valid_user_agents.update(line.strip() for line in f if line.strip())

    for i, (vin, url) in enumerate(rows, 1):
        listing_start = datetime.now()

        soup = fetch_with_retries(url, ua)
        if soup is None:
            try:
                soup = fetch_with_selenium(url)
            except Exception as se:
                print(f"❌ Selenium failed for {vin}: {se}")
                retry_list.append((vin, url))
                continue

        try:
            if check_listing_still_active(soup):
                price = extract_price(soup)
                if price:
                    print(f"✅ VIN {vin} — active, new price: ${price}")
                    cur.execute("UPDATE listings SET price = ?, last_seen = ? WHERE vin = ?", (price, today, vin))
                    cur.execute("INSERT INTO price_history (vin, date, price) VALUES (?, ?, ?)", (vin, today, price))
                else:
                    print(f"✅ VIN {vin} — active, price not found")
                    cur.execute("UPDATE listings SET last_seen = ? WHERE vin = ?", (today, vin))
            else:
                print(f"❌ VIN {vin} — listing removed")
                cur.execute("UPDATE listings SET status = 'inactive' WHERE vin = ?", (vin,))

            conn.commit()
            time.sleep(random.uniform(0.5, 1.5))

        except Exception as e:
            print(f"⚠️ Error processing {vin}: {e}")
            retry_list.append((vin, url))

        # ETA Calculation
        elapsed = (datetime.now() - start_time).total_seconds()
        avg_time = elapsed / i
        remaining = len(rows) - i
        eta_seconds = int(avg_time * remaining)
        eta_min = eta_seconds // 60
        eta_sec = eta_seconds % 60
        print(f"⏳ Estimated time remaining: {eta_min}m {eta_sec}s")

    conn.close()

    if retry_list:
        print(f"\n🔁 {len(retry_list)} listings failed. Consider retrying:")
        for vin, url in retry_list:
            print(f" - {vin}: {url}")

    new_valid_user_agents = used_user_agents - failed_user_agents
    if new_valid_user_agents:
        print("\n✅ Valid User-Agents (used successfully without failure):")
        with open(VALID_UA_LOG, "a", encoding="utf-8") as f:
            for agent in new_valid_user_agents:
                print(f" - {agent}")
                f.write(agent + "\n")