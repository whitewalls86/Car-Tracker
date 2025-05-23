import sqlite3
from datetime import date
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from queue import Queue
from config import DB_PATH
from collections import deque, Counter
from page_fetcher import fetch_soup_with_fallback


recent_request_types = deque(maxlen=100)


def check_listing_still_active(soup):
    return soup.select_one("spark-notification.unlisted-notification[open]") is None


def extract_price(soup):
    el = soup.select_one("span.primary-price")
    price = int(el.text.strip().replace("$", "").replace(",", "")) if el and "$" in el.text else None
    return price

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
        soup, request_type = fetch_soup_with_fallback(url, 10)
        if soup is None:
            print(f"{vin} error.")
            return vin, None, None, None
        if soup and not check_listing_still_active(soup):
            return vin, "inactive", None, request_type

        price = extract_price(soup)
        return vin, "active", price, request_type

    db_thread = threading.Thread(target=db_update_worker, daemon=True)
    db_thread.start()

    start_time = time.time()
    total = len(rows)

    with ThreadPoolExecutor(max_workers=24) as executor:
        futures = {executor.submit(verify_listing, vin, url): vin for vin, url in rows}
        recent_times = deque(maxlen=100)
        for i, future in enumerate(as_completed(futures), 1):
            vin, status, price, request_type = future.result()
            if request_type:
                recent_request_types.append(request_type)
            if status is not None:
                update_queue.put((vin, status, price))
            method_counts = Counter(recent_request_types)
            summary = " | ".join(f"{m}: {c}%" for m, c in method_counts.items())
            now = time.time()
            elapsed = int(now - start_time)
            elapsed_min, elapsed_sec = divmod(elapsed, 60)
            recent_times.append(now)
            avg_time_per_vin = (recent_times[-1] - recent_times[0]) / len(recent_times) if len(recent_times) > 1 else 0
            remaining = int(avg_time_per_vin * (total - i))
            eta_min, eta_sec = divmod(remaining, 60)
            percent = (i / total) * 100
            print(f"\r Verifying {i}/{total} ({percent:.1f}%) | Elapsed: {elapsed_min}m {elapsed_sec}s | ETA: {eta_min}m {eta_sec}s | {summary}.", end="", flush=True)

    update_queue.join()
    update_queue.put(None)
    db_thread.join()
    print("\n Verification complete.")
