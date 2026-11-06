# deadlock_fixed.py — global lock order buffer_lock -> stats_lock eliminates circular wait
import threading, time, sys

CAPACITY     = int(sys.argv[1]) if len(sys.argv) > 1 else 5
NPRODUCERS   = int(sys.argv[2]) if len(sys.argv) > 2 else 3
NCONSUMERS   = int(sys.argv[3]) if len(sys.argv) > 3 else 3
ITEMS_PER_P  = int(sys.argv[4]) if len(sys.argv) > 4 else 100

buffer       = []
buffer_lock  = threading.Lock()
stats_lock   = threading.Lock()
stats        = {"produced": 0, "consumed": 0}
metrics = {"prod_block_s": 0.0, "cons_block_s": 0.0}
m_lock = threading.Lock()

def producer(pid):
    for i in range(ITEMS_PER_P):
        t0 = time.perf_counter()
        buffer_lock.acquire()
        blocked = 0.0
        while len(buffer) >= CAPACITY:
            buffer_lock.release(); time.sleep(0.001)
            blocked += 0.001; buffer_lock.acquire()
        with m_lock: metrics["prod_block_s"] += blocked
        buffer.append((pid, i))
        stats_lock.acquire()            # still second, but SAME order everywhere
        stats["produced"] += 1
        stats_lock.release()
        buffer_lock.release()

def consumer(cid):
    total = NPRODUCERS * ITEMS_PER_P
    while True:
        blocked = 0.0
        buffer_lock.acquire()           # REVERSED from flawed version
        stats_lock.acquire()
        if stats["consumed"] >= total:
            stats_lock.release(); buffer_lock.release(); return
        if not buffer:
            stats_lock.release(); buffer_lock.release()
            time.sleep(0.001); blocked += 0.001
            with m_lock: metrics["cons_block_s"] += blocked
            continue
        buffer.pop(0); stats["consumed"] += 1
        stats_lock.release(); buffer_lock.release()

if __name__ == "__main__":
    t0 = time.perf_counter()
    ts = ([threading.Thread(target=producer, args=(i,)) for i in range(NPRODUCERS)] +
          [threading.Thread(target=consumer, args=(i,)) for i in range(NCONSUMERS)])
    for t in ts: t.start()
    for t in ts: t.join()
    elapsed = time.perf_counter() - t0
    total   = NPRODUCERS * ITEMS_PER_P
    print(f"capacity={CAPACITY}  P={NPRODUCERS}  C={NCONSUMERS}  items={total}")
    print(f"elapsed      = {elapsed:6.3f} s")
    print(f"throughput   = {total/elapsed:7.1f} items/s")
    print(f"avg producer blocking = {metrics['prod_block_s']*1000/total:6.2f} ms/item")
    print(f"avg consumer blocking = {metrics['cons_block_s']*1000/total:6.2f} ms/item")
    print("Watchdog not needed — completed successfully (no deadlock).")