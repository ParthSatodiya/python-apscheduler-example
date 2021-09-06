from datetime import time
from apscheduler.executors.pool import ThreadPoolExecutor
from apscheduler.job import Job
from apscheduler.schedulers.background import BlockingScheduler
from apscheduler.events import EVENT_JOB_ADDED, EVENT_JOB_ERROR, EVENT_JOB_EXECUTED, JobEvent, JobExecutionEvent
import time
import random
import signal

# Test task to schedule
def add(x, y, sleep=1):
    time.sleep(sleep)
    assert x is not None
    assert y is not None
    print(f"{x} + {y} = {x + y}")

def scheduler_listener(event: JobEvent):
    print(f"Job ({event.job_id}) added, args: {scheduler.get_job(event.job_id).args}")

def job_listener(event: JobExecutionEvent):
    if event.exception:
        print(f"Job {event.job_id} crashed!")
    # else:  
    #     print(f"Job {event.job_id} done!")

if __name__ == "__main__":
    executors = {
        'default': ThreadPoolExecutor(11),
    }
    job_defaults = {
        'coalesce': False,
        'max_instances': 1,
    }
    scheduler = BlockingScheduler(executors=executors, job_defaults=job_defaults)

    # Schedule multiple tasks with no trigger 
    # to simulate scenario where tasks will be executed at the same time with limited threads
    for i in range(10):
        scheduler.add_job(add, args=[i, random.randint(21,40), 2])
    scheduler.add_job(add, args=[None, random.randint(21,40)])      # Fail test
    for i in range(10):
        scheduler.add_job(add, args=[10+i, random.randint(21,40)])

    # Add listeners to print added jobs and their execution results
    scheduler.add_listener(scheduler_listener, EVENT_JOB_ADDED)
    scheduler.add_listener(job_listener, EVENT_JOB_ERROR | EVENT_JOB_EXECUTED)

    # capture signal to gracefully shutdown the scheduler 
    def signal_handler(sig, frame):
        print('Shutting down...')
        scheduler.shutdown()
    signal.signal(signal.SIGINT, signal_handler)

    scheduler.start()