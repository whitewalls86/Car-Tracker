from typing import List
from job import Job, PrioritizedJobQueue, SharedState
from page_fetcher import fetch_soup_with_fallback
from config import BASE_URL, PAGE_SIZE
from urllib.parse import urlencode


class PageLoadJob(Job):
    """
    Job to load a specific results page, extract vehicle cards, and enqueue them for ID resolution.
    """
    def __init__(self, page_num: int, makes: List[str], models: List[str], scope: str, zip_code: str, radius: int,
                 shared_state: SharedState):
        self.page_num = page_num
        self.makes = makes
        self.models = models
        self.scope = scope
        self.zip_code = zip_code
        self.radius = radius
        self.shared_state = shared_state

    def run(self, job_queue: PrioritizedJobQueue) -> None:
        tracker = self.shared_state.tracker
        tracker.record_start(self.__class__.__name__)

        params = {
            "makes[]": self.makes,
            "models[]": self.models,
            "stock_type": "new",
            "zip": self.zip_code,
            "page": self.page_num,
            "page_size": PAGE_SIZE,
            "maximum_distance": self.radius if self.scope == "local" else "all"
        }
        url = BASE_URL + "?" + urlencode(params, doseq=True)
        soup, _ = fetch_soup_with_fallback(url)

        if not soup:
            print(f"[PageLoadJob] Failed to fetch page {self.page_num}")
            self.shared_state.dispatcher.notify_page_complete()
            return

        cards = soup.select("div.vehicle-card")
        for card in cards:
            listing_id = card.get("data-listing-id")
            if listing_id:
                self.shared_state.dispatcher.add_unresolved_listing(listing_id, card)

        self.shared_state.dispatcher.notify_page_complete()
        tracker.record_complete(self.__class__.__name__)
