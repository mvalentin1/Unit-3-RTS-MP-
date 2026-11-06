# queue_sim.py — bounded queue ≈ RTOS message queue (Section 3.4)
import queue, threading, time, statistics

q = queue.Queue(maxsize=5)
latencies = []
lat_lock = threading.Lock()
CONSUMER_DELAY = 0.0   # set >0 to see blocking increase

def producer_task():
    for i in range(20):
        item = (i, time.perf_counter())
        q.put(item)                      # blocks automatically when full
    q.put(None)

def consumer_task():
    while True:
        item = q.get()
        if item is None: break
        idx, sent = item
        recv = time.perf_counter()
        with lat_lock:
            latencies.append((recv - sent) * 1000)  # ms
        if CONSUMER_DELAY: time.sleep(CONSUMER_DELAY)

if __name__ == "__main__":
    p = threading.Thread(target=producer_task)
    c = threading.Thread(target=consumer_task)
    p.start(); c.start(); p.join(); c.join()
    print(f"min={min(latencies):.3f} max={max(latencies):.3f} "
          f"avg={statistics.mean(latencies):.3f} ms  (n={len(latencies)})")