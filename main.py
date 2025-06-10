from job import PrioritizedJobQueue, Worker, SharedState, StopJob
from jobs.dispatcher import Dispatcher
from jobs.page_loader import PageLoadJob
from config import SEARCH_CONFIG
from db import init_db
from status_tracker import StatusTracker
from utils.job_utils import enqueue_with_priority


NUM_WORKERS = 32


def main():
    init_db()

    shared_state = SharedState(batch_size=200)
    tracker = StatusTracker()
    shared_state.tracker = tracker

    tracker.start_loop()

    job_queue = PrioritizedJobQueue()
    workers = [Worker(job_queue) for _ in range(NUM_WORKERS)]
    for w in workers:
        w.start()

    zip_code = SEARCH_CONFIG["zip"]
    radius = SEARCH_CONFIG["radius"]
    total_pages = SEARCH_CONFIG["pages"]
    models = SEARCH_CONFIG["models"]

    for entry in models:
        make = entry["make"]
        model = entry["model"]
        shared_state.dispatcher = Dispatcher(job_queue, shared_state, total_pages)

        for page_num in range(1, total_pages + 1):
            enqueue_with_priority(job_queue, PageLoadJob(
                page_num=page_num,
                makes=[make],
                models=[model],
                scope="local",
                zip_code=zip_code,
                radius=radius,
                shared_state=shared_state
            ))

            enqueue_with_priority(job_queue, PageLoadJob(
                page_num=page_num,
                makes=[make],
                models=[model],
                scope="national",
                zip_code=zip_code,
                radius=radius,
                shared_state=shared_state
            ))

    job_queue.join()

    for _ in workers:
        job_queue.put(StopJob())
    for w in workers:
        w.join()

    tracker.stop()


if __name__ == "__main__":
    main()
