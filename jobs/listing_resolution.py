from typing import List, Tuple
from job import Job, PrioritizedJobQueue, SharedState
from db import get_vins_by_listing_ids
from jobs.card_processing import SaveJob, DetailScrapeJob  # SaveJob submits listings to the batch buffer
from bs4 import Tag
from utils.soup_helpers import extract_price
from utils.job_utils import enqueue_with_priority


class ListingIDResolutionJob(Job):
    """
    Resolves whether listings exist in the DB and queues DetailScrapeJobs or SaveJobs accordingly.
    """
    def __init__(self, batch: List[Tuple[str, Tag]], shared_state: SharedState):
        self.batch = batch  # List of (listing_id, card)
        self.shared_state = shared_state

    def run(self, job_queue: PrioritizedJobQueue) -> None:
        tracker = self.shared_state.tracker
        tracker.record_start(self.__class__.__name__)

        listing_ids = [listing_id for listing_id, _ in self.batch]
        existing_map = get_vins_by_listing_ids(listing_ids)  # {listing_id: vin}

        for listing_id, card in self.batch:
            self.shared_state.add_seen_listing_id(listing_id)

            if listing_id in existing_map:
                price = extract_price(card)

                enqueue_with_priority(job_queue, SaveJob({
                    "vin": existing_map[listing_id],
                    "listing_id": listing_id,
                    "price": price,
                    "search_scope": self.shared_state.scope,
                    "distance": None,
                    "shipping_cost": None,
                }, self.shared_state))
            else:
                enqueue_with_priority(job_queue, DetailScrapeJob(listing_id, card, shared_state=self.shared_state))

        tracker.record_complete(self.__class__.__name__)


