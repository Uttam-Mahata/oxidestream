import os
import random

categories = ['action', 'comedy', 'drama', 'sci-fi']
users = ['user_1', 'user_2', 'user_3', 'user_4', 'user_5']

def generate_partition(filename, num_rows):
    os.makedirs('tests/data', exist_ok=True)
    filepath = os.path.join('tests/data', filename)
    with open(filepath, 'w') as f:
        f.write("user_id,rating,timestamp,category\n")
        for i in range(num_rows):
            user = random.choice(users)
            rating = random.randint(1, 5)
            timestamp = f"2026-06-04T12:{random.randint(10,59):02d}:{random.randint(10,59):02d}Z"
            category = random.choice(categories)
            f.write(f"{user},{rating},{timestamp},{category}\n")

# Generate ~60 rows per partition to get ~2.5KB size
generate_partition('part-0.csv', 60)
generate_partition('part-1.csv', 60)
generate_partition('part-2.csv', 60)

# Generate category_lookup.csv
category_path = os.path.join('tests/data', 'category_lookup.csv')
with open(category_path, 'w') as f:
    f.write("category,category_name\n")
    f.write("action,Action & Adventure\n")
    f.write("comedy,Comedy Specials\n")
    f.write("drama,Drama Series\n")
    f.write("sci-fi,Sci-Fi & Fantasy\n")

print("Data generation complete.")
