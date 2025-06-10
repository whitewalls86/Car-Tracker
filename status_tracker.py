from collections import defaultdict, deque
import threading
import time

from rich.console import Console
from rich.table import Table
from rich.live import Live

console = Console()


class JobStatus:
    def __init__(self):
        self.created = 0
        self.completed = 0
        self.total_time = 0.0
        self.start_times = deque()
        self.lock = threading.Lock()

    def job_started(self):
        with self.lock:
            self.created += 1
            self.start_times.append(time.time())

    def job_completed(self):
        with self.lock:
            self.completed += 1
            if self.start_times:
                start = self.start_times.popleft()
                self.total_time += time.time() - start

    def get_stats(self):
        with self.lock:
            avg_time = self.total_time / self.completed if self.completed else 0
            remaining = self.created - self.completed
            eta = remaining * avg_time
            return {
                "created": self.created,
                "completed": self.completed,
                "avg_time": avg_time,
                "eta": eta
            }


class StatusTracker:
    def __init__(self):
        self.jobs = defaultdict(JobStatus)
        self.running = False

    def record_start(self, job_type: str):
        self.jobs[job_type].job_started()

    def record_complete(self, job_type: str):
        self.jobs[job_type].job_completed()

    def start_loop(self, interval=1.0):
        self.running = True
        threading.Thread(target=self._loop, args=(interval,), daemon=True).start()

    def stop(self):
        self.running = False

    def _loop(self, interval):
        with Live(self.render(), refresh_per_second=4, console=console) as live:
            while self.running:
                live.update(self.render())
                time.sleep(interval)

    def render(self):
        table = Table(show_header=True, header_style="bold magenta")
        table.add_column("Job Type", style="cyan")
        table.add_column("Created", justify="right")
        table.add_column("Done", justify="right")
        table.add_column("Avg Time", justify="right")
        table.add_column("ETA", justify="right")

        for job_type, status in self.jobs.items():
            stats = status.get_stats()
            eta_m, eta_s = divmod(int(stats["eta"]), 60)
            eta_fmt = f"{eta_m}m {eta_s}s" if stats["eta"] > 0 else "--"
            table.add_row(
                job_type,
                str(stats["created"]),
                str(stats["completed"]),
                f"{stats['avg_time']:.1f}s",
                eta_fmt
            )
        return table
