from datetime import date
from config import ENQUEUE_BATCH_SIZE
from job import Job, PrioritizedJobQueue, SharedState
from db import get_all_active_listing_ids
from jobs.card_processing import SaveJob
from utils.job_utils import enqueue_with_priority


class VerifierJob(Job):
    """
    Compares active listings in DB that haven't been seen today and queues
    verification detail scrapes for each.
    """
    def __init__(self, shared_state: SharedState, today: date = date.today()):
        self.shared_state = shared_state
        self.today = today

    def run(self, job_queue: PrioritizedJobQueue) -> None:
        tracker = self.shared_state.tracker
        tracker.record_start(self.__class__.__name__)

        stale_listings = get_all_active_listing_ids(today=self.today)
        self.shared_state.verifier_queue = stale_listings

        # Add one job that will start feeding the details
        enqueue_with_priority(job_queue, VerifierProducerJob(self.shared_state))

        tracker.record_complete(self.__class__.__name__)


class VerifyDetailJob(Job):
    """
    Performs a detail scrape using just the VIN + URL to see if a previously active listing is still valid.
    If the listing is inactive, it updates the DB to mark it as such.
    If active, it updates last_seen and optionally price.
    """
    def __init__(self, vin: str, url: str, shared_state: SharedState):
        self.vin = vin
        self.url = url
        self.shared_state = shared_state

    def run(self, job_queue: PrioritizedJobQueue) -> None:
        from page_fetcher import fetch_soup_with_fallback
        from utils.soup_helpers import check_listing_still_active, extract_price
        from datetime import date

        tracker = self.shared_state.tracker
        tracker.record_start(self.__class__.__name__)

        today = date.today()
        soup, request_type = fetch_soup_with_fallback(self.url, 10)

        if soup is None:
            print(f"[VerifyDetailJob] {self.vin} — error during fetch.")
            return

        if not check_listing_still_active(soup):
            listing = {"vin": self.vin, "status": "inactive"}
            enqueue_with_priority(job_queue, SaveJob(listing, self.shared_state))
            return

        price = extract_price(soup)
        listing = {
            "vin": self.vin,
            "last_seen": today,
        }
        if price:
            listing["price"] = price

        enqueue_with_priority(job_queue, SaveJob(listing, self.shared_state))
        tracker.record_complete(self.__class__.__name__)


class VerifierProducerJob(Job):
    def __init__(self, shared_state: SharedState):
        self.shared_state = shared_state

    def run(self, job_queue: PrioritizedJobQueue) -> None:

        tracker = self.shared_state.tracker
        tracker.record_start(self.__class__.__name__)

        try:
            listings = self.shared_state.verifier_queue
        except AttributeError:
            print("[VerifierProducerJob] No verifier_queue found.")
            return

        # Pull off a chunk and enqueue detail jobs
        for _ in range(min(ENQUEUE_BATCH_SIZE, len(listings))):
            vin, url = listings.pop()
            enqueue_with_priority(job_queue, VerifyDetailJob(vin, url, self.shared_state))
            tracker.record_complete(self.__class__.__name__)

        # If there's still more work to do, enqueue another round
        if listings:
            enqueue_with_priority(job_queue, VerifierProducerJob(self.shared_state))

        tracker.record_complete(self.__class__.__name__)

