import time
import random
import requests
import statistics
from concurrent.futures import ThreadPoolExecutor, as_completed

# Configuration
SERVICE_URL = "http://localhost:5000"
NUM_ACCOUNTS = 100
HOTSPOT_FRACTION = 0.2
HOTSPOT_LOAD = 0.8
REQUEST_RATE = 500       # requests per second
DURATION = 60            # seconds
WRITE_RATIO = 0.3        # fraction of lock requests

# Precompute hotspot and normal account IDs
hotspot_ids = list(range(int(NUM_ACCOUNTS * HOTSPOT_FRACTION)))
normal_ids = list(range(int(NUM_ACCOUNTS * HOTSPOT_FRACTION), NUM_ACCOUNTS))

def choose_account_id():
    return random.choice(hotspot_ids) if random.random() < HOTSPOT_LOAD else random.choice(normal_ids)

def send_request(session):
    account_id = choose_account_id()
    if random.random() < WRITE_RATIO:
        resp = session.post(f"{SERVICE_URL}/accounts/{account_id}/lock")
    else:
        resp = session.get(f"{SERVICE_URL}/accounts/{account_id}/balance")
    j = resp.json()
    return j["result"], j.get("latency_ms", 0)

def run_workload():
    end = time.time() + DURATION
    results = []
    with ThreadPoolExecutor(max_workers=100) as exec:
        session = requests.Session()
        futures = []
        # generate REQUEST_RATE requests per second in 0.1s batches
        while time.time() < end:
            for _ in range(REQUEST_RATE // 10):
                futures.append(exec.submit(send_request, session))
            time.sleep(0.1)
        # collect
        for f in as_completed(futures):
            try:
                results.append(f.result())
            except:
                pass

    total = len(results)
    collisions = sum(1 for r,_ in results if r=="COLLISION")
    lats = [lat for _,lat in results]
    dead_lock_rate = collisions/total*100 if total else 0
    rollback_rate = collisions/total*10000 if total else 0
    p50 = statistics.median(lats) if lats else 0
    p95 = statistics.quantiles(lats, n=100)[94] if len(lats)>=100 else max(lats, default=0)
    p99 = statistics.quantiles(lats, n=100)[98] if len(lats)>=100 else max(lats, default=0)
    throughput = total/DURATION

    print(f"Total requests: {total}")
    print(f"Dead-lock rate: {dead_lock_rate:.2f}%")
    print(f"Rollback rate (/10k): {rollback_rate:.0f}")
    print(f"Latency p50/p95/p99: {p50:.1f}/{p95:.1f}/{p99:.1f} ms")
    print(f"Throughput: {throughput:.1f} req/s")

if __name__=="__main__":
    run_workload()
