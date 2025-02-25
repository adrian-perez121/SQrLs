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

def evaluate_insert(query, num_records, num_columns):
    start_time = time.time()
    for i in range(num_records):
        query.insert(*[i] * num_columns)
    duration = time.time() - start_time
    results["Insert"].append((num_records, duration))

def evaluate_update(query, num_records, column_index):
    start_time = time.time()
    for i in range(num_records):
        query.update(i, *[None if j != column_index else i * 2 for j in range(query.table.num_columns)])
    duration = time.time() - start_time
    results["Update"].append((num_records, duration))

def evaluate_select(query, num_records, column_index):
    start_time = time.time()
    for i in range(num_records):
        query.select(i, query.table.key, [1 if j == column_index else 0 for j in range(query.table.num_columns)])
    duration = time.time() - start_time
    results["Select"].append((num_records, duration))

def evaluate_sum(query, start_key, end_key, column_index):
    start_time = time.time()
    query.sum(start_key, end_key, column_index)
    duration = time.time() - start_time
    results["Sum"].append((end_key - start_key + 1, duration))

def evaluate_delete(query, num_records):
    start_time = time.time()
    for i in range(num_records):
        query.delete(i)
    duration = time.time() - start_time
    results["Delete"].append((num_records, duration))

# initialize db
db = Database()
db.open("db_location")
table = db.create_table("test_table", 5, 0)
query = Query(table)

num_columns = 5
column_index = 2

# run different query types
for num_records in [100, 1000, 10000, 100000]:
    evaluate_insert(query, num_records, num_columns)
    evaluate_update(query, num_records, column_index)
    evaluate_select(query, num_records, column_index)
    evaluate_sum(query, 0, num_records - 1, column_index)
    evaluate_delete(query, num_records)

db.close()

# plot
fig, axes = plt.subplots(3, 2, figsize=(12, 10))
fig.suptitle("Query Performance", fontsize=16)

axes = axes.flatten()

x_ticks = [100, 1000, 10000, 100000]
y_ticks = [0.001, 0.01, 0.1, 1, 10, 50]

i = 0
for key, data in results.items():
    x_vals = [d[0] for d in data]
    y_vals = [d[1] for d in data]
    
    axes[i].plot(x_vals, y_vals, marker='o', linestyle='-', color=colors[key], label=key)
    axes[i].set_xscale("log")
    axes[i].set_yscale("log")

    axes[i].set_xticks(x_ticks)
    axes[i].set_xticklabels([str(tick) for tick in x_ticks])

    axes[i].set_yticks(y_ticks)
    axes[i].set_yticklabels([str(tick) for tick in y_ticks])

    axes[i].set_title(key, fontsize=14, fontweight="bold", color=colors[key])
    axes[i].set_xlabel("Number of Queries")
    axes[i].set_ylabel("Execution Time (s)")
    axes[i].grid(True, linestyle="--", alpha=0.6)
    axes[i].legend()
    i += 1

plt.tight_layout()
plt.subplots_adjust(top=0.9)
plt.show()
