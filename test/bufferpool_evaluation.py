import sys
import os
import time
import threading
import matplotlib.pyplot as plt

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from lstore.bufferpool import BufferPool, Frame
from lstore.page_range import PageRange

results = {
    "Request Frame": [],
    "Write Frame": [],
    "Evict Frame": [],
    "Read Frame": [],
}

colors = {
    "Request Frame": "#007bff",
    "Write Frame": "#ff0000",
    "Evict Frame": "#28a745",
    "Read Frame": "#ff9900",
}

lock = threading.Lock()

# ----------- REQUEST FRAME -----------
def evaluate_request_frame(buffer_pool, num_frames):
    start_time = time.time()
    times = []

    for i in range(num_frames):
        t0 = time.time()
        buffer_pool.get_frame("test_table", i, num_columns=5)
        times.append(time.time() - t0)

    duration = time.time() - start_time
    results["Request Frame"].append((num_frames, duration))

    print(f"Requested {num_frames} frames in {duration:.4f} seconds.")
    print(f"Avg time per request: {sum(times)/len(times):.6f} sec, Max: {max(times):.6f}, Min: {min(times):.6f}")


# ----------- WRITE FRAME -----------
def evaluate_write_frame(buffer_pool, num_frames):
    """Measures time taken to write frames to disk."""
    start_time = time.time()

    for i in range(num_frames):
        frame = Frame("test_table", i, PageRange(5))
        buffer_pool.write_frame(frame)

    duration = time.time() - start_time
    results["Write Frame"].append((num_frames, duration))
    print(f"Wrote {num_frames} frames to disk in {duration:.4f} seconds.")

# ----------- EVICT FRAME -----------
def evaluate_evict_frame(buffer_pool, num_frames):
    start_time = time.time()
    failures = 0

    for _ in range(num_frames):
        try:
            buffer_pool.evict_frame()
        except Exception as e:
            print(f"Eviction failed: {e}")
            failures += 1

    duration = time.time() - start_time
    results["Evict Frame"].append((num_frames, duration))
    print(f"Evicted {num_frames - failures} frames successfully in {duration:.4f} seconds.")


# ----------- READ FRAME -----------
def evaluate_read_frame(buffer_pool, num_frames):
    """Measures time taken to read frames from disk."""
    start_time = time.time()

    for i in range(num_frames):
        try:
            buffer_pool.read_frame("test_table", i, num_columns=5)
        except Exception as e:
            print(f"Read failed for frame {i}: {e}")

    duration = time.time() - start_time
    results["Read Frame"].append((num_frames, duration))
    print(f"Read {num_frames} frames from disk in {duration:.4f} seconds.")

# ----------- BUFFER POOL SETUP -----------
buffer_pool = BufferPool("bufferpool_test_dir")
frame_counts = [50, 100, 500, 1000] 

# buffer pool evaluations
for num_frames in frame_counts:
    print(f"\nRunning Buffer Pool Evaluation for {num_frames} frames...")
    evaluate_request_frame(buffer_pool, num_frames)
    evaluate_write_frame(buffer_pool, num_frames)
    evaluate_evict_frame(buffer_pool, num_frames)
    evaluate_read_frame(buffer_pool, num_frames)

# ----------- PLOT TOTAL EXECUTION TIME -----------

# Compute total execution times per frame count
total_times = []
frame_sizes = [50, 100, 500, 1000]

for i in range(len(frame_sizes)):
    total_time = (
        results["Request Frame"][i][1] +
        results["Write Frame"][i][1] +
        results["Evict Frame"][i][1] +
        results["Read Frame"][i][1]
    )
    total_times.append(total_time)

# Create a single plot for total execution time across different frame sizes
plt.figure(figsize=(10, 6))
plt.plot(frame_sizes, total_times, marker='o', linestyle='-', color="#007bff", label="Total Execution Time")

# Formatting the plot
plt.xticks(frame_sizes, ["50", "100", "500", "1000"])
plt.xlabel("Number of Frames")
plt.ylabel("Total Execution Time (s)")
plt.title("Total Buffer Pool Time")
plt.grid(True, linestyle="--", alpha=0.6)
plt.legend()

# Display the plot
plt.show()


