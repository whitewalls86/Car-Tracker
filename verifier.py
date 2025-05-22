import os
os.environ["TF_CPP_MIN_LOG_LEVEL"] = "3"
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
from webdriver_manager.chrome import ChromeDriverManager
import cloudscraper
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from queue import Queue
from config import DB_PATH
from collections import deque

FAILED_UA_LOG = "failed_user_agents.log"
SUCCESS_UA_LOG = "successful_user_agents.log"
VALID_UA_LOG = "valid_user_agents.txt"

failed_user_agents = set()
used_user_agents = set()
valid_user_agents = set()


def check_listing_still_active(soup):
    return soup.select_one("spark-notification.unlisted-notification[open]") is None


def extract_price(soup):
    el = soup.select_one("span.primary-price")
    return int(el.text.strip().replace("$", "").replace(",", "")) if el and "$" in el.text else None


def fetch_with_selenium(url):
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920x1080")
    service = Service(executable_path=ChromeDriverManager().install(), log_path="/dev/null")  # Use "NUL" on Windows
    driver = webdriver.Chrome(service=service, options=options)
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
                time.sleep(random.uniform(0.5, 1.0))
            else:
                try:
                    scraper = cloudscraper.create_scraper()
                    res = scraper.get(url, timeout=10)
                    return BeautifulSoup(res.text, "html.parser")
                except Exception:
                    return None


def verify_active_listings():
    today = date.today()
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute("""
        SELECT vin, url FROM listings
        WHERE status = 'active' AND last_seen < ?
    """, (today,))
    rows = cur.fetchall()
    conn.close()

    print(f"Verifying {len(rows)} listings...")

    update_queue = Queue()
    ua = UserAgent()

    def db_update_worker():
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        while True:
            task = update_queue.get()
            if task is None:
                break
            vin, status, price = task
            if status == "inactive":
                cur.execute("UPDATE listings SET status = 'inactive' WHERE vin = ?", (vin,))
            else:
                if price:
                    cur.execute("UPDATE listings SET price = ?, last_seen = ? WHERE vin = ?", (price, today, vin))
                    cur.execute("INSERT INTO price_history (vin, date, price) VALUES (?, ?, ?)", (vin, today, price))
                else:
                    cur.execute("UPDATE listings SET last_seen = ? WHERE vin = ?", (today, vin))
            conn.commit()
            update_queue.task_done()
        conn.close()

    def verify_listing(vin, url):
        soup = fetch_with_retries(url, ua)
        if soup is None:
            try:
                soup = fetch_with_selenium(url)
            except:
                return vin, None, None
        if soup and not check_listing_still_active(soup):
            return vin, "inactive", None
        price = extract_price(soup)
        return vin, "active", price

    db_thread = threading.Thread(target=db_update_worker, daemon=True)
    db_thread.start()

    start_time = time.time()
    total = len(rows)

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(verify_listing, vin, url): vin for vin, url in rows}
        recent_times = deque(maxlen=100)
        for i, future in enumerate(as_completed(futures), 1):
            vin, status, price = future.result()
            if status is not None:
                update_queue.put((vin, status, price))
            now = time.time()
            elapsed = int(now - start_time)
            recent_times.append(now)
            avg_time_per_vin = (recent_times[-1] - recent_times[0]) / len(recent_times) if len(recent_times) > 1 else 0
            remaining = int(avg_time_per_vin * (total - i))
            eta_min, eta_sec = divmod(remaining, 60)
            percent = (i / total) * 100
            print(f"\r Verifying {i}/{total} ({percent:.1f}%) | Elapsed: {elapsed}s | ETA: {eta_min}m {eta_sec}s", end="", flush=True)

    update_queue.join()
    update_queue.put(None)
    db_thread.join()
    print("\n Verification complete.")
