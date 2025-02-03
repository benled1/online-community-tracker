from collections import defaultdict
from typing import Any

import pandas as pd
import psycopg2
from cosine_similarity import _plot_similarity_matplot, compute_3d_cords

conn = psycopg2.connect(
    dbname="postgres",
    user="postgres",
    password="password",
    host="localhost",
    port="5432"
)

cur = conn.cursor()
cur.execute("SELECT * FROM chat_messages LIMIT 14000")
data: list[tuple[Any]] = cur.fetchall()

for row in data:
    print(row)

# dictionary to store group vectors
group_data = defaultdict(lambda: defaultdict(int))
all_users = set()

# process the data to populate the dictionary
for _, username, _, group, _ in data:
    group_data[group][username] += 1
    all_users.add(username)
for group in group_data:
    for user in all_users:
        group_data[group].setdefault(user, 0)

group_data = {group: dict(user_counts) for group, user_counts in group_data.items()}

import pprint
pprint.pprint(group_data)

cords_df: pd.DataFrame = compute_3d_cords(group_data)
_plot_similarity_matplot(cords_df)