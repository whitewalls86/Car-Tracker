from abc import ABC, abstractmethod
from queue import PriorityQueue
from threading import Thread, Lock
from typing import Set, List, Dict, Tuple
from itertools import count


class Job(ABC):
    """
    Abstract base class for all jobs in the processing pipeline.
    """
    @abstractmethod
    def run(self, job_queue: 'PrioritizedJobQueue') -> None:
        pass


class StopJob(Job):
    """
    Sentinel job to signal a worker to stop.
    """
    def run(self, job_queue: 'PrioritizedJobQueue') -> None:
        raise StopIteration


class PrioritizedJobQueue(PriorityQueue):
    """
    A simple alias for Queue to indicate job-specific use.
    """
    def __init__(self):
        super().__init__()
        self.counter = count()
        self.lock = Lock()

    def put_job(self, job, priority: int):
        with self.lock:
            order = next(self.counter)
            super().put((priority, order, job))


class Worker(Thread):
    """
    Thread worker that pulls and executes jobs from the job queue.
    """
    def __init__(self, job_queue: PrioritizedJobQueue):
        super().__init__(daemon=True)
        self.job_queue = job_queue

    def run(self):
        while True:
            priority, order, job = self.job_queue.get()
            try:
                job.run(self.job_queue)
            except StopIteration:
                break
            except Exception as e:
                print(f"[Worker Error] {e}")
            finally:
                self.job_queue.task_done()


class ListingBuffer:
    """
    Thread-safe buffer for batching listings before saving to the database.
    """
    def __init__(self, batch_size: int):
        self.batch_size = batch_size
        self.buffer: List[Dict] = []
        self.lock = Lock()

    def add(self, listing: Dict) -> bool:
        """
        Add a listing to the buffer. Returns True if buffer reached batch size.
        """
        with self.lock:
            self.buffer.append(listing)
            return len(self.buffer) >= self.batch_size

    def flush(self) -> List[Dict]:
        """
        Flush and return the current buffer contents.
        """
        with self.lock:
            to_flush = self.buffer[:]
            self.buffer.clear()
            return to_flush


class UnresolvedListingBuffer:
    """
    Thread-safe buffer for batching unresolved listing IDs and their card HTML.
    Used to check which listings exist in the DB.
    """
    def __init__(self, batch_size: int):
        self.batch_size = batch_size
        self.buffer: List[Tuple[str, object]] = []  # (listing_id, card)
        self.lock = Lock()

    def add(self, listing_id: str, card: object) -> bool:
        """
        Add an unresolved listing. Returns True if buffer reached batch size.
        """
        with self.lock:
            self.buffer.append((listing_id, card))
            return len(self.buffer) >= self.batch_size

    def flush(self) -> List[Tuple[str, object]]:
        """
        Flush and return the buffer contents.
        """
        with self.lock:
            to_flush = self.buffer[:]
            self.buffer.clear()
            return to_flush


class SharedState:
    """
    Container for shared mutable state across jobs.
    """
    def __init__(self, batch_size: int = 100):
        self.seen_listing_ids: Set[str] = set()
        self.seen_lock = Lock()

        self.listing_buffer = ListingBuffer(batch_size=batch_size)
        self.unresolved_buffer = UnresolvedListingBuffer(batch_size=batch_size)

        self.dispatcher = None  # Will be assigned after initialization
        self.tracker = None  # Optional StatusTracker Instance
        self.scope = None
        self.verifier_queue = None

    def add_seen_listing_id(self, listing_id: str) -> None:
        with self.seen_lock:
            self.seen_listing_ids.add(listing_id)

    def was_seen(self, listing_id: str) -> bool:
        with self.seen_lock:
            return listing_id in self.seen_listing_ids
