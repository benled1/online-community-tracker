from datetime import datetime

import pandas as pd
from pymongo import MongoClient

from cosine_similarity import _plot_similarity_matplot, compute_3d_cords


def get_channel_vectors(
    start_time: datetime,
    end_time: datetime,
    mongo_uri: str = "mongodb://localhost:27017",
    db_name: str = "chat_db",
    collection_name: str = "chat_messages"
) -> dict:
    """
    Retrieve all chat messages from the specified MongoDB collection between start_time and end_time,
    and return a dictionary mapping each channel to a chat vector.

    The chat vector is a dictionary where:
      - Keys are every username that chatted in ANY channel during the period.
      - Values are the counts of messages that user sent in that specific channel.

    Args:
        start_time (datetime): The start of the time period (inclusive).
        end_time (datetime): The end of the time period (exclusive).
        mongo_uri (str): MongoDB connection URI.
        db_name (str): Name of the database.
        collection_name (str): Name of the collection containing chat messages.

    Returns:
        dict: A dictionary mapping each channel (str) to a chat vector (dict of {username: count}).
    """
    # Connect to MongoDB
    client = MongoClient(mongo_uri)
    db = client[db_name]
    collection = db[collection_name]

    # Query for messages within the time period.
    query = {"timestamp": {"$gte": start_time, "$lt": end_time}}

    # Retrieve the set of unique users (sender_name) that chatted in any channel during the period.
    unique_users = collection.distinct("sender_name", query)
    unique_users = sorted(unique_users)  # Sorted for consistency (optional).

    # Retrieve the list of unique channels that have chat messages in the period.
    unique_channels = collection.distinct("channel", query)

    # Initialize a chat vector for each channel: every user set to 0.
    channel_vectors = {channel: {user: 0 for user in unique_users} for channel in unique_channels}

    # Iterate over messages and update the chat vector counts.
    for doc in collection.find(query):
        channel = doc.get("channel")
        sender = doc.get("sender_name")
        # Ensure both channel and sender are present in our dictionaries
        if channel in channel_vectors and sender in channel_vectors[channel]:
            channel_vectors[channel][sender] += 1

    # Close the connection
    client.close()

    return channel_vectors

if __name__ == "__main__":
    start = datetime(2025, 2, 8, 0, 0, 0)
    end = datetime(2025, 2, 9, 0, 0, 0)

    vectors = get_channel_vectors(start, end)

    cords_3d: pd.DataFrame = compute_3d_cords(vectors)
    print(cords_3d)
    _plot_similarity_matplot(cords_3d)