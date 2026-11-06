# isr_sim.py — strict-priority preemptive bottom-half scheduling (Section 3.5)
# Usage: python isr_sim.py [config] [duration_s]
#   config 0 = short top halves throughout
#   config 1 = highest-priority source has a LONG top half
#   config 2 = same as 0, but highest-priority source period halved
import threading, time, heapq, statistics, sys

CONFIG   = int(sys.argv[1]) if len(sys.argv) > 1 else 0
DURATION = float(sys.argv[2]) if len(sys.argv) > 2 else 10.0

# (name, priority, period_s, isr_dur_s, bh_dur_s)
SOURCES = [
    ("UART",  3, 0.010, 0.0005, 0.0010),
    ("TIMER", 2, 0.020, 0.0005, 0.0020),
    ("ADC",   1, 0.030, 0.0005, 0.0030),
]
if CONFIG == 1:
    SOURCES[0] = ("UART",  3, 0.010, 0.0050, 0.0010)   # long top half
if CONFIG == 2:
    SOURCES[0] = ("UART",  3, 0.005, 0.0005, 0.0010)   # faster arrivals

# ---- shared state ----
bh_heap = []                          # (-priority, seq, src, assert_ts, remaining_s)
seq = 0
heap_lock = threading.Lock()
heap_cv   = threading.Condition(heap_lock)
running   = True
results   = {name: [] for name, *_ in SOURCES}
res_lock  = threading.Lock()

def top_half(src, priority, isr_dur, bh_dur, assert_ts):
    # The top half itself is modelled as an uninterruptible short delay.
    time.sleep(isr_dur)
    global seq
    with heap_cv:
        seq += 1
        heapq.heappush(bh_heap, (-priority, seq, src, assert_ts, bh_dur))
        heap_cv.notify()

def cpu_worker():
    # Always runs the HIGHEST-priority ready bottom half; can be preempted
    # by a newly arrived higher-priority one mid-execution.
    while True:
        with heap_cv:
            while not bh_heap and running:
                heap_cv.wait(timeout=0.05)
            if not bh_heap and not running:
                return
            if not bh_heap:
                continue
            negp, s, src, assert_ts, remaining = heapq.heappop(bh_heap)
        # simulate execution in small slices so preemption can occur
        slice_s = 0.0002
        finished = True
        while remaining > 0:
            with heap_cv:
                higher_ready = bool(bh_heap) and -bh_heap[0][0] > -negp
                if higher_ready:
                    seq_holder = s
                    heapq.heappush(bh_heap, (negp, seq_holder + 1, src, assert_ts, remaining))
                    finished = False
                    break
            step = min(remaining, slice_s)
            time.sleep(step)
            remaining -= step
        if finished:
            latency_ms = (time.perf_counter() - assert_ts) * 1000.0
            with res_lock:
                results[src].append(latency_ms)

def source_thread(name, priority, period, isr_dur, bh_dur, stop_at):
    while time.perf_counter() < stop_at:
        time.sleep(period)
        assert_ts = time.perf_counter()
        top_half(name, priority, isr_dur, bh_dur, assert_ts)

def summarize():
    print(f"\nConfig {CONFIG} — {DURATION}s run")
    print(f"{'source':6} {'n':>5} {'min':>8} {'mean':>8} {'p95':>8} {'max':>8}   (ms)")
    for name, *_ in SOURCES:
        xs = sorted(results[name])
        if not xs:
            print(f"{name:6} {'0':>5}  (no samples)")
            continue
        p95 = xs[int(0.95 * len(xs))]
        print(f"{name:6} {len(xs):>5} "
              f"{min(xs):>8.3f} {statistics.mean(xs):>8.3f} "
              f"{p95:>8.3f} {max(xs):>8.3f}")

if __name__ == "__main__":
    stop_at = time.perf_counter() + DURATION
    cpu  = threading.Thread(target=cpu_worker, daemon=True)
    cpu.start()
    src_threads = [threading.Thread(target=source_thread,
                                    args=(*s, stop_at)) for s in SOURCES]
    for t in src_threads: t.start()
    for t in src_threads: t.join()
    time.sleep(0.3)          # let the worker drain
    running = False
    with heap_cv: heap_cv.notify_all()
    cpu.join(timeout=1.0)
    summarize()


