# deadlock.py — flawed lock order reproduces deadlock (Sections 3.4, 3.8)
import threading, time, sys

CAPACITY     = int(sys.argv[1]) if len(sys.argv) > 1 else 5
NPRODUCERS   = int(sys.argv[2]) if len(sys.argv) > 2 else 3
NCONSUMERS   = int(sys.argv[3]) if len(sys.argv) > 3 else 3
ITEMS_PER_P  = int(sys.argv[4]) if len(sys.argv) > 4 else 100

buffer       = []
buffer_lock  = threading.Lock()
stats_lock   = threading.Lock()
stats        = {"produced": 0, "consumed": 0}
progress     = {"count": 0}
progress_lock = threading.Lock()

def bump():
    with progress_lock:
        progress["count"] += 1

def producer(pid):
    for i in range(ITEMS_PER_P):
        # ---- FLAWED ORDER: buffer_lock THEN stats_lock ----
        buffer_lock.acquire()
        while len(buffer) >= CAPACITY:
            buffer_lock.release()
            time.sleep(0.001)
            buffer_lock.acquire()
        buffer.append((pid, i))
        stats_lock.acquire()                      # <-- hold buffer, wait stats
        stats["produced"] += 1
        stats_lock.release()
        buffer_lock.release()
        bump()
        time.sleep(0.0005)

def consumer(cid):
    total = NPRODUCERS * ITEMS_PER_P
    while True:
        # ---- FLAWED ORDER: stats_lock THEN buffer_lock ----
        stats_lock.acquire()                      # <-- hold stats, wait buffer
        if stats["consumed"] >= total:
            stats_lock.release(); return
        buffer_lock.acquire()
        if not buffer:
            buffer_lock.release(); stats_lock.release()
            time.sleep(0.001); continue
        buffer.pop(0)
        stats["consumed"] += 1
        buffer_lock.release()
        stats_lock.release()
        bump()
        time.sleep(0.0005)

def watchdog(dead):
    last = -1; stalled = 0
    while not dead.is_set():
        time.sleep(0.5)
        with progress_lock:
            cur = progress["count"]
        if cur == last:
            stalled += 1
            if stalled >= 4:                      # ~2 s with no progress
                print("\n*** WATCHDOG: no progress for ~2 s — deadlock suspected ***")
                print(f"    buffer_lock: {'HELD' if buffer_lock.locked() else 'free'}")
                print(f"    stats_lock : {'HELD' if stats_lock.locked() else 'free'}")
                print("    (both held simultaneously ⇒ producers/consumers in a cycle)")
                dead.set()
                return
        else:
            stalled = 0; last = cur

if __name__ == "__main__":
    dead = threading.Event()
    wd = threading.Thread(target=watchdog, args=(dead,), daemon=True)
    wd.start()
    ts = ([threading.Thread(target=producer, args=(i,)) for i in range(NPRODUCERS)] +
          [threading.Thread(target=consumer, args=(i,)) for i in range(NCONSUMERS)])
    for t in ts: t.start()
    for t in ts: t.join(timeout=10)
    if dead.is_set():
        print("Run ended: deadlock reproduced. Exiting.")
    else:
        print("Run ended: completed without deadlock (lucky this time).")