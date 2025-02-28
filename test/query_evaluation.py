import sys
import os
import time
import random
import threading
import matplotlib.pyplot as plt
import numpy as np

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from lstore.db import Database
from lstore.query import Query

results = {
    "Insert": [],
    "Update": [],
    "Select": [],
    "Sum": [],
    "Delete": [],
}

colors = {
    "Insert": "#007bff",
    "Update": "#ff0000",
    "Select": "#28a745",
    "Sum": "#ff9900",
    "Delete": "#6f42c1",
}

inserted_keys = set()
lock = threading.Lock()

# ----------- INSERT -----------
def evaluate_insert(query, num_records, num_columns):
    """Perform batch inserts of varying sizes."""
    start_time = time.time()

    batch_sizes = [1, 10, 100]  # random batch sizes
    inserted = 0

    while inserted < num_records:
        batch_size = min(random.choice(batch_sizes), num_records - inserted)
        batch_values = [[random.randint(1, 100000) for _ in range(num_columns)] for _ in range(batch_size)]

        with lock:
            for values in batch_values:
                pk = values[0]  # unique primary keys
                if pk not in inserted_keys:
                    query.insert(*values)
                    inserted_keys.add(pk)
                    inserted += 1

    duration = time.time() - start_time
    results["Insert"].append((num_records, duration))
    print(f"Inserted {inserted} unique records.")

# ----------- UPDATE -----------
def evaluate_tail_update(query, num_records, column_index):
    """Update a subset of inserted records, ensuring proportional scaling."""
    start_time = time.time()

    with lock:
        total_records = len(inserted_keys)
        update_count = max(100, min(total_records // 5, total_records))  # update 20% of dataset (min 100)

        updated_keys = random.sample(list(inserted_keys), update_count) if inserted_keys else []

    for pk in updated_keys:
        complexity = random.choice([3, query.table.num_columns])  # force at least 3 columns
        update_values = [random.randint(1, 10000) if j < complexity else None for j in range(query.table.num_columns)]
        query.update(pk, *update_values)

    duration = time.time() - start_time
    results["Update"].append((num_records, duration))
    print(f"Updated {len(updated_keys)} records.")

# ----------- SELECT -----------
def evaluate_complex_select(query, num_records):
    """Perform batch select queries."""
    start_time = time.time()

    with lock:
        selected_keys = random.sample(list(inserted_keys), min(num_records, len(inserted_keys)))

    for pk in selected_keys:
        complexity = random.choice([1, 3, query.table.num_columns])  # retrieve varying # of columns
        query.select(pk, query.table.key, [1 if i < complexity else 0 for i in range(query.table.num_columns)])

    duration = time.time() - start_time
    results["Select"].append((num_records, duration))

# ----------- SUM -----------
def evaluate_sum(query, start_key, end_key, column_index):
    """Perform sum queries with dynamically growing range sizes, ensuring proper scaling."""
    start_time = time.time()

    with lock:
        if inserted_keys:
            total_records = len(inserted_keys)
            min_key = min(inserted_keys)

            if total_records <= 100:
                max_key = min_key + min(10, total_records)  # select all when dataset is tiny
            else:
                range_type = random.choice(["small", "medium", "large"])

                if range_type == "small":
                    max_key = min_key + max(10, total_records // 10)  # 10% of dataset
                elif range_type == "medium":
                    max_key = min_key + max(50, total_records // 3)  # 30% of dataset
                else:  # Large
                    max_key = min_key + max(100, total_records // 2)  # 50% of dataset

                max_key = min(max_key, max(inserted_keys))  # safe to not exceed dataset size

            print(f"SUM Query: Range {min_key} - {max_key} ({max_key - min_key} records)")
            
            # ✅ FIX: Capture the result of `query.sum()`
            result = query.sum(min_key, max_key, column_index)
            
            # ✅ PRINT the actual summed value
            print(f"SUM Query Result: {result}")

    duration = time.time() - start_time
    results["Sum"].append((end_key - start_key + 1, duration))


# ----------- DELETE -----------
def evaluate_delete(query, num_records):
    """Delete all records efficiently in bulk."""
    start_time = time.time()

    with lock:
        delete_count = len(inserted_keys)
        delete_candidates = list(inserted_keys)

    for pk in delete_candidates:
        query.delete(pk)

    with lock:
        inserted_keys.clear()

    duration = time.time() - start_time
    results["Delete"].append((num_records, duration))
    print(f"Deleted {delete_count} records in {duration:.2f} seconds.")

# ----------- DATABASE SETUP -----------
db = Database()
db.open("db_location")
table = db.create_table("test_table", 5, 0)
query = Query(table)

num_columns = 5
column_index = 2

# ----------- RUN QUERIES -----------
for num_records in [100, 1000, 10000, 100000]:
    print(f"Running {num_records} queries...")

    evaluate_insert(query, num_records, num_columns)
    evaluate_tail_update(query, num_records, column_index)
    evaluate_complex_select(query, num_records)
    evaluate_sum(query, 0, num_records - 1, column_index)
    evaluate_delete(query, num_records)

db.close()

# ----------- PLOT RESULTS -----------
fig, axes = plt.subplots(3, 2, figsize=(14, 10))
axes = axes.flatten()
fig.suptitle("Query Performance", fontsize=16, fontweight="bold")

for i, (key, data) in enumerate(results.items()):
    if not data:
        continue

    x_vals = [d[0] for d in data]  # frames
    y_vals = [d[1] for d in data]  # time(s)

    axes[i].plot(x_vals, y_vals, marker='o', linestyle='-', color=colors[key], label=key)

    axes[i].set_xticks([100, 1000, 10000, 100000])
    axes[i].set_xticklabels(["", "1K", "10K", "100K"])

    axes[i].set_title(key, fontsize=14, fontweight="bold", color=colors[key])
    axes[i].set_xlabel("Number of Queries")
    axes[i].set_ylabel("Execution Time (s)")
    axes[i].grid(True, linestyle="--", alpha=0.6)
    axes[i].legend()

plt.tight_layout()
plt.subplots_adjust(top=0.9)
plt.show()
