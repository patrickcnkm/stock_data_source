
# workers/scheduler.py
from apscheduler.schedulers.blocking import BlockingScheduler
import subprocess, os

def job_streams_demo():
    # Publish last 2 days from staging to streams (simulate realtime)
    subprocess.Popen(["python", "workers/streams_gateway.py", "--symbols", "00700.HK", "AAPL", "--days", "2", "--sleep-ms", "2"])

def job_strategy_runner():
    subprocess.Popen(["python", "workers/strategy_runner.py", "--symbols", "00700.HK", "AAPL", "--group", "strategy_alpha_v1", "--consumer", "c1"])

def job_rotation_demo():
    subprocess.Popen(["python", "workers/sub_rotation.py", "--demo", "--pinned", "00700.HK", "AAPL", "--batch-size", "6", "--hold-sec", "75"])

def main():
    sched = BlockingScheduler(timezone="UTC")
    # kick off once at start
    sched.add_job(job_rotation_demo, "date")
    sched.add_job(job_streams_demo, "date")
    sched.add_job(job_strategy_runner, "date")
    # You can add cron jobs here for nightly ingest/validate/commit
    sched.start()

if __name__ == "__main__":
    main()
