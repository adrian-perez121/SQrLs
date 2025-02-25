import sys
import os
import time
import matplotlib.pyplot as plt
import numpy as np

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from lstore.db import Database
from lstore.query import Query

# initialize result dir
results = {
    "Buffer Pool Frames": [],
    "Merge Threshold": [],
    "Base Pages per Range": [],
    "Page Size": [],
}

buffer_pool_sizes = [50, 250, 500, 750, 1000] 
merge_thresholds = [50, 500, 2000, 4000, 8000]  # expo(?) increase for merge frequency
base_pages_per_range = [10, 40, 80, 160, 320]  # double base pages per range
page_sizes = [64, 128, 256, 512, 1024, 2048]  # consistent ^2 progression

query_counts = 100000  # inc to approach 10-20s per test
agg_batch_size = 100  # aggregation batch fixed


if os.path.exists("db_location"):
    print("Removing old database directory...\n")
    os.system("rm -rf db_location")

print("Initializing fresh database...\n")
db = Database()
db.open("db_location")
table = db.create_table("test_table", 5, 0)
query = Query(table)

print("Database successfully initialized!\n")
print("Starting Buffer Pool & Merge Evaluation...\n")

# helpers to execute multiple query types
def execute_mixed_workload():
    # Insert
    for i in range(query_counts):
        query.insert(i, 1, 2, 3, 4)

    # Update (modify first column, heavier load)
    for i in range(query_counts):  
        query.update(i, 100, None, None, None, None)

    # Select (inc workload)
    for i in range(query_counts):  
        query.select(i, 0, [1, 1, 1, 1, 1])

    # Delete (quarter of records)
    for i in range(query_counts // 4):
        query.delete(i)

    # Aggregation (test workload)
    start = 0
    end = query_counts // 10  
    for _ in range(10):  
        query.sum(start, end, 1)
        start += agg_batch_size
        end += agg_batch_size

def test_buffer_pool_size():
    print("Running Buffer Pool Size Test...\n")
    for size in buffer_pool_sizes:
        db.bufferpool.capacity = size
        start_time = time.time()
        execute_mixed_workload()
        duration = time.time() - start_time
        results["Buffer Pool Frames"].append((size, duration))
        print(f"Completed buffer size {size} in {duration:.4f} seconds")

def test_merge_threshold():
    print("\nRunning Merge Threshold Test...\n")
    for threshold in merge_thresholds:
        db.bufferpool.capacity = 500
        start_time = time.time()
        execute_mixed_workload()
        duration = time.time() - start_time
        results["Merge Threshold"].append((threshold, duration))
        print(f"Completed merge threshold {threshold} in {duration:.4f} seconds")

def test_page_ranges():
    print("\nRunning Base Pages per Range Test...\n")
    for base_pages in base_pages_per_range:
        start_time = time.time()
        execute_mixed_workload()
        duration = time.time() - start_time
        results["Base Pages per Range"].append((base_pages, duration))
        print(f"Completed base pages {base_pages} in {duration:.4f} seconds")

def test_page_sizes():
    print("\nRunning Page Size Test...\n")
    for size in page_sizes:
        start_time = time.time()
        execute_mixed_workload()
        duration = time.time() - start_time
        results["Page Size"].append((size, duration))
        print(f"Completed page size {size} in {duration:.4f} seconds")

# run tests
test_buffer_pool_size()
test_merge_threshold()
test_page_ranges()
test_page_sizes()
db.close()

# graph settings
fig, axes = plt.subplots(2, 2, figsize=(12, 10))
fig.suptitle("Buffer Pool & Page Query Performance", fontsize=16)
axes = axes.flatten()

graph_titles = [
    "Time vs. Buffer Pool Frames",
    "Time vs. Merge Threshold",
    "Time vs. Base Pages per Range",
    "Time vs. Page Size"
]

x_labels = ["Frames", "Merge Threshold", "Base Pages", "Page Size"]
colors = ["blue", "red", "green", "purple"]

# plot
for i, (key, data) in enumerate(results.items()):
    x_vals = [d[0] for d in data]
    y_vals = [d[1] for d in data]

    axes[i].plot(x_vals, y_vals, marker='o', linestyle='-', color=colors[i])
    axes[i].set_title(graph_titles[i], fontsize=14, color=colors[i])
    axes[i].set_xlabel(x_labels[i])
    axes[i].set_ylabel("Time (s)")
    axes[i].grid(True, linestyle="--", alpha=0.6)

    axes[i].ticklabel_format(style='plain', axis='y')

plt.tight_layout()
plt.subplots_adjust(top=0.9)
plt.show()
