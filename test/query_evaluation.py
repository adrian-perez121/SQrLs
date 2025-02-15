import sys
import os
import time
import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation
import numpy as np

# par dir for path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from lstore.db import Database
from lstore.query import Query

# result dictionary
results = {
    "Insert": [],
    "Update": [],
    "Select": [],
    "Sum": [],
    "Delete": [],
}

# color dictionary
colors = {
    "Insert": "#39FF14",
    "Update": "#FF007F",
    "Select": "#00FFFF",
    "Sum": "#C04DFF",
    "Delete": "#E2E603",
}

# eval functions
def evaluate_insert(query, num_records, num_columns):
    print(f"Evaluating Insert, {num_records} records...")
    start_time = time.time()
    for i in range(num_records):
        query.insert(*[i] * num_columns)  # temp data
    end_time = time.time()
    duration = end_time - start_time
    results["Insert"].append((num_records, duration))
    print(f"Inserting {num_records} records | Total time: {duration} seconds")

def evaluate_update(query, num_records, column_index):
    print(f"Evaluating Update, {num_records} records...")
    start_time = time.time()
    for i in range(num_records):
        query.update(i, *[None if j != column_index else i * 2 for j in range(query.table.num_columns)])
    end_time = time.time()
    duration = end_time - start_time
    results["Update"].append((num_records, duration))
    print(f"Updating {num_records} records | Total time: {duration} seconds")

def evaluate_select(query, num_records, column_index):
    print(f"Evaluating Sum, {num_records} records...")
    start_time = time.time()
    for i in range(num_records):
        result = query.select(i, query.table.key, [1 if j == column_index
                else 0 for j in range(query.table.num_columns)])
        assert result, f"Failed selecting record {i}"
    end_time = time.time()
    duration = end_time - start_time
    results["Select"].append((num_records, duration))
    print(f"Selecting {num_records} records | Total time: {duration} seconds")

def evaluate_sum(query, start_key, end_key, column_index):
    print(f"Evaluating Sum within {start_key} - {end_key}")
    start_time = time.time()
    total = query.sum(start_key, end_key, column_index)
    end_time = time.time()
    duration = end_time - start_time
    results["Sum"].append((end_key - start_key + 1, duration))
    print(f"Summing records within range: {start_key} - {end_key} | Total time: {duration} seconds | Total Sum: {total}")

def evaluate_delete(query, num_records):
    print(f"Evaluating Delete, {num_records} records...")
    start_time = time.time()
    for i in range(num_records):
        query.delete(i)
    end_time = time.time()
    duration = end_time - start_time
    results["Delete"].append((num_records, duration))
    print(f"Deleting {num_records} records | Total time: {duration} seconds")

# visualization
fig, ax = plt.subplots(figsize=(10, 7))
fig.patch.set_facecolor('#1B1B2F')
ax.set_facecolor('#1B1B2F')
ax.set_xlabel("Number of Queries", color="white")
ax.set_ylabel("Execution Time (s)", color="white")
ax.set_title("Query Performance", color="white")
ax.set_xscale("linear")
ax.set_yscale("linear")
ax.grid(True, linestyle="--", alpha=0.4, color="white")

# axis limits
ax.set_xlim(100, 1000000)
ax.set_ylim(0.000001, 20)

# ticks
ax.set_xticks([0, 200000, 400000, 600000, 800000, 1000000])
ax.set_xticklabels([0, 200000, 400000, 600000, 800000, 1000000], color="white")
ax.set_yticks([0, 2, 4, 6, 8, 10])
ax.set_yticklabels(["0", "2", "4", "6", "8", "10"], color="white")

# animated lines
lines = {
    key: ax.plot([], [], label = key, color = colors[key], linewidth = 2)[0]
    for key in results
}

# smooth left to right line
def create_line(data, num_points):
    record_counts = np.array([d[0] for d in data])
    times = np.array([d[1] for d in data])
    if len(record_counts) < 2:
        return record_counts, times
    x_new = np.logspace(np.log10(record_counts[0]), np.log10(record_counts[-1]), num_points)
    y_new = np.interp(np.log10(x_new), np.log10(record_counts), times)
    return x_new, y_new

# animation function
def update(frame):
    for key, line in lines.items():
        data = results[key]
        if len(data) > 1:
            x = np.array([d[0] for d in data])
            y = np.array([d[1] for d in data])
            line.set_data(x[:frame], y[:frame])  # real data
    ax.legend(loc="upper left", fontsize=10, facecolor="#1B1B2F", edgecolor="white", labelcolor="white")
    return lines.values()


if __name__ == "__main__":
    db = Database()
    db.open("db_location")
    table = db.create_table("test_table", 5, 0)

    query = Query(table)

    # test para
    num_columns = 5
    column_index = 2

    # generate test data
    for num_records in [100, 1000, 10000, 100000, 1000000]:
        evaluate_insert(query, num_records, num_columns)
        evaluate_update(query, num_records, column_index)
        evaluate_select(query, num_records, column_index)
        evaluate_sum(query, 0, num_records - 1, column_index)
        evaluate_delete(query, num_records)

    # define animation frames
    max_frames = 100
    pause_frames = 2 * 60  # pause after completion for 2s
    total_frames = max_frames + pause_frames

    def extended_update(frame):
        if frame < max_frames:
            return update(frame)
        return lines.values()

    # animation output
    animation = FuncAnimation(fig, extended_update, frames=total_frames, interval=16.67, blit=True)
    animation.save("query_performance_neon.gif", writer="pillow", fps=60, dpi=300)
    plt.show()
    db.close()
