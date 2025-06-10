from job import PrioritizedJobQueue, SharedState
from jobs.listing_resolution import ListingIDResolutionJob
from jobs.verifier import VerifierJob
from utils.job_utils import enqueue_with_priority


class Dispatcher:
    """
    Coordinates flushing unresolved listings and triggering verification jobs
    once all pages have been processed.
    """
    def __init__(self, job_queue: PrioritizedJobQueue, shared_state: SharedState, total_pages: int):
        self.job_queue = job_queue
        self.shared_state = shared_state
        self.remaining_pages = total_pages
        self.lock = shared_state.seen_lock  # reuse lock to guard page countdown

    def add_unresolved_listing(self, listing_id: str, card: object) -> None:
        should_flush = self.shared_state.unresolved_buffer.add(listing_id, card)
        if should_flush:
            unresolved_batch = self.shared_state.unresolved_buffer.flush()
            enqueue_with_priority(self.job_queue, ListingIDResolutionJob(unresolved_batch, self.shared_state))

    def notify_page_complete(self) -> None:
        with self.lock:
            self.remaining_pages -= 1
            if self.remaining_pages == 0:
                # Final flush of any unresolved listings
                final_batch = self.shared_state.unresolved_buffer.flush()
                if final_batch:
                    enqueue_with_priority(self.job_queue, ListingIDResolutionJob(final_batch, self.shared_state))

                enqueue_with_priority(self.job_queue, VerifierJob(self.shared_state))
