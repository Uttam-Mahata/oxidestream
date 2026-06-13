import os
import random

categories = ['action', 'comedy', 'drama', 'sci-fi']
users = ['user_1', 'user_2', 'user_3', 'user_4', 'user_5']

def generate_partition(filename, num_rows):
    filepath = os.path.join('/home/uttam/oxidestream/tests/data', filename)
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
print("Data generation complete.")
