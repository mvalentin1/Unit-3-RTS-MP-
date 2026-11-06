# timing_overhead.py — measures thread-start→event-set latency as dispatch proxy (Section 3.5)
import threading, time, statistics

def one_round(intervals):
    ev = threading.Event()
    t0 = time.perf_counter()
    th = threading.Thread(target=ev.set)
    th.start()
    ev.wait()
    t1 = time.perf_counter()
    th.join()
    intervals.append((t1 - t0) * 1e6)   # microseconds

if __name__ == "__main__":
    intervals = []
    for _ in range(200):
        one_round(intervals)
    intervals.sort()
    p95 = intervals[int(0.95 * len(intervals))]
    print(f"min={min(intervals):.1f}us mean={statistics.mean(intervals):.1f}us "
          f"p95={p95:.1f}us max={max(intervals):.1f}us  (worst-case dispatch latency)")