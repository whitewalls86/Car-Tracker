import threading
import time
import os
from collections import defaultdict
from datetime import datetime
import page_fetcher


class StatusTracker:
    def __init__(self):
        self.status_table = defaultdict(dict)
        self.lock = threading.Lock()
        self.running = False
        self.thread = None

    def register_model(self, make, model):
        key = (make, model)
        with self.lock:
            self.status_table[key] = {
                "pages_scraped": 0,
                "vins_seen": 0,
                "new_vins": 0,
                "updated_vins": 0,
                "card_queue": 0,
                "db_queue": 0,
                "start_time": time.time(),
                "initial_requests": page_fetcher.total_requests_made,
                "initial_bytes": page_fetcher.total_bytes_downloaded,
                "last_scraping": False,
            }

    def update(self, make, model, **kwargs):
        key = (make, model)
        with self.lock:
            for k, v in kwargs.items():
                self.status_table[key][k] = v

    def start_refresh_loop(self, interval=1.0):
        self.running = True
        self.thread = threading.Thread(target=self._refresh_loop, args=(interval,), daemon=True)
        self.thread.start()

    def stop(self):
        self.running = False
        if self.thread:
            self.thread.join()

    def _refresh_loop(self, interval):
        while self.running:
            self._render()
            time.sleep(interval)

    def _render(self):
        os.system('cls' if os.name == 'nt' else 'clear')
        with self.lock:
            print("| Make     | Model               | Pages | VINs | New | Upd | CardQ | DBQ | Time   | Req | MB   |")
            print("|----------|---------------------|-------|------|-----|-----|--------|-----|--------|-----|------|")
            for (make, model), data in self.status_table.items():
                elapsed = int(time.time() - data["start_time"])
                mins, secs = divmod(elapsed, 60)
                reqs = page_fetcher.total_requests_made - data.get("initial_requests", 0)
                mb = (page_fetcher.total_bytes_downloaded - data.get("initial_bytes", 0)) / 1024 / 1024
                print(f"| {make:<8} | {model:<19} | {data['pages_scraped']:>3}/40 | {data['vins_seen']:>4} | {data['new_vins']:>3} | {data['updated_vins']:>3} |  {data['card_queue']:>5} | {data['db_queue']:>3} | {mins:>2}m {secs:02}s | {reqs:>3} | {mb:>5.2f} |")

            # Find active scraping model
            for (make, model), data in self.status_table.items():
                if data.get("last_scraping"):
                    print(f"\n🔄 Currently scraping: {make}-{model} | Page {data['pages_scraped']} | Cards: {data['card_queue']} | DB Queue: {data['db_queue']} | VINs: {data['vins_seen']} (New: {data['new_vins']}, Updated: {data['updated_vins']})")
                    break
