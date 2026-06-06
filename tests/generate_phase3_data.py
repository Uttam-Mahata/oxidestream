import sqlite3
import os

os.makedirs("tests/data", exist_ok=True)

# 1. Create SQLite database for user metadata (Ecosystem Connector test)
db_path = "tests/data/user_metadata.db"
if os.path.exists(db_path):
    os.remove(db_path)

conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# Create table user_status
cursor.execute("""
CREATE TABLE user_status (
    user_id TEXT,
    age INTEGER,
    membership TEXT
)
""")

# Insert records matching our user IDs
cursor.execute("INSERT INTO user_status VALUES ('user_1', 25, 'Premium')")
cursor.execute("INSERT INTO user_status VALUES ('user_2', 30, 'Free')")
cursor.execute("INSERT INTO user_status VALUES ('user_3', 35, 'Premium')")
cursor.execute("INSERT INTO user_status VALUES ('user_4', 40, 'Premium')")
cursor.execute("INSERT INTO user_status VALUES ('user_5', 22, 'Free')")

conn.commit()
conn.close()

print("Phase 3 SQLite database generated successfully.")
