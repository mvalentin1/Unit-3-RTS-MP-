# semaphore_pool.py — counting semaphore = N interchangeable resources (Section 3.4)
import threading, time, random

POOL = 3
pool_semaphore = threading.Semaphore(POOL)
holding = 0
hold_lock = threading.Lock()
max_concurrent = 0
wait_times = []
wt_lock = threading.Lock()

def task(tid):
    global holding, max_concurrent
    t0 = time.perf_counter()
    pool_semaphore.acquire()
    wait = time.perf_counter() - t0
    with hold_lock:
        holding += 1
        max_concurrent = max(max_concurrent, holding)
    with wt_lock:
        wait_times.append((tid, wait))
    time.sleep(random.uniform(0.001, 0.005))   # hold resource briefly
    with hold_lock:
        holding -= 1
    pool_semaphore.release()

if __name__ == "__main__":
    ts = [threading.Thread(target=task, args=(i,)) for i in range(8)]
    for t in ts: t.start()
    for t in ts: t.join()
    print(f"Max concurrent holders: {max_concurrent} (should be ≤ {POOL})")
    for tid, w in sorted(wait_times, key=lambda x: x[1]):
        print(f"  task {tid}: wait={w*1000:.2f} ms")