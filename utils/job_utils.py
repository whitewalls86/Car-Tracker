from config import JOB_PRIORITIES


def enqueue_with_priority(job_queue, job):
    job_type = job.__class__.__name__
    priority = JOB_PRIORITIES.get(job_type, 10)
    job_queue.put_job(job, priority)
