# race_fixed.py — fixes race with threading.Lock (mutex-like, Section 3.4)
import threading
import time

shared_counter = 0
counter_lock = threading.Lock()

def increment_safe(iterations):
    global shared_counter
    for _ in range(iterations):
        with counter_lock:             # critical section
            temp = shared_counter
            time.sleep(0.0001)
            shared_counter = temp + 1

if __name__ == "__main__":
    threads = [threading.Thread(target=increment_safe, args=(1000,))
               for _ in range(5)]
    for t in threads: t.start()
    for t in threads: t.join()
    print(f"Final counter: {shared_counter} (expected 5000)")