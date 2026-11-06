# race_demo.py — reproduces lost-update race condition (Section 3.8)
import threading
import time

shared_counter = 0  # module scope, NO lock

def increment_unsafe(iterations):
    global shared_counter
    for _ in range(iterations):
        temp = shared_counter          # READ
        time.sleep(0.0001)             # widen interleaving window
        shared_counter = temp + 1      # WRITE (stale copy!)

if __name__ == "__main__":
    threads = [threading.Thread(target=increment_unsafe, args=(1000,))
               for _ in range(5)]
    for t in threads: t.start()
    for t in threads: t.join()
    print(f"Final counter: {shared_counter} (expected 5000)")