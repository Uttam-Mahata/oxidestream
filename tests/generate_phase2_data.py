import csv
import os

os.makedirs("tests/data", exist_ok=True)

# 1. Generate Linear Regression Data: y = 2*x1 + 3*x2
# columns: x1, x2, y
lr_path = "tests/data/lr_data.csv"
with open(lr_path, "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["x1", "x2", "y"])
    # 4 points to define a perfect plane
    writer.writerow([1.0, 1.0, 5.0])
    writer.writerow([1.0, 2.0, 8.0])
    writer.writerow([2.0, 1.0, 7.0])
    writer.writerow([2.0, 2.0, 10.0])

# 2. Generate PageRank Data
# columns: target, rank, out_degree
pr_path = "tests/data/pagerank_data.csv"
with open(pr_path, "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["target", "rank", "out_degree"])
    # A simple triangle network node ranks
    writer.writerow(["node_A", 1.0, 2.0])
    writer.writerow(["node_B", 1.0, 2.0])
    writer.writerow(["node_C", 1.0, 1.0])

# 3. Generate DPP Dimension Table (Users status)
# columns: user_id, active, region
dim_path = "tests/data/dim_user.csv"
with open(dim_path, "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["user_id", "active", "region"])
    writer.writerow(["user_1", "true", "US"])
    writer.writerow(["user_2", "false", "EU"])
    writer.writerow(["user_3", "true", "US"])

# 4. Generate DPP Fact Table (Large ratings data)
# columns: user_id, rating, timestamp, category
# Make it > 50KB to trigger partitions and avoid CBO broadcast
fact_path = "tests/data/ratings_dpp.csv"
with open(fact_path, "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["user_id", "rating", "timestamp", "category"])
    # Write 2500 rows to ensure size is around 60KB
    categories = ["action", "comedy", "drama", "sci-fi"]
    for i in range(2500):
        uid = f"user_{(i % 3) + 1}"  # user_1, user_2, user_3
        rating = (i % 5) + 1
        timestamp = 1780598000 + i
        category = categories[i % 4]
        writer.writerow([uid, rating, timestamp, category])

print("Phase 2 test datasets generated successfully.")
