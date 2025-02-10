#!/usr/bin/env python3
import matplotlib.pyplot as plt
import numpy as np
from mpl_toolkits.mplot3d import Axes3D  # Needed for 3D plotting
from pymongo import MongoClient
from sklearn.manifold import MDS


def get_channel_user_sets(mongo_uri="mongodb://localhost:27017",
                          db_name="chat_db",
                          collection_name="chat_messages"):
    """
    Connects to MongoDB and aggregates the set of unique users per channel.
    Returns a dictionary: {channel: set(users)}
    """
    client = MongoClient(mongo_uri)
    db = client[db_name]
    collection = db[collection_name]

    pipeline = [
        {
            "$group": {
                "_id": "$channel",
                "users": {"$addToSet": "$sender_name"}
            }
        }
    ]
    results = list(collection.aggregate(pipeline))
    channel_users = {}
    for result in results:
        channel = result["_id"]
        users = set(result["users"])
        channel_users[channel] = users

    client.close()
    return channel_users

def custom_distance_exponential(set1, set2, k=100):
    """
    Computes a custom distance using an exponential transformation.
    1. Compute the raw Jaccard similarity: |A∩B| / |A∪B|
    2. Transform it: transformed similarity = 1 - exp(-k * raw_similarity)
    3. Return distance = 1 - transformed similarity (which simplifies to exp(-k * raw_similarity))
    """
    if not set1 and not set2:
        return 0.0
    jaccard_sim = len(set1.intersection(set2)) / len(set1.union(set2))
    transformed_sim = 1 - np.exp(-k * jaccard_sim)
    return 1 - transformed_sim  # equivalently, np.exp(-k * jaccard_sim)

def compute_distance_matrix(channel_users, distance_func, **kwargs):
    """
    Given a dictionary mapping channels to user sets,
    compute a symmetric distance matrix using the provided distance function.
    Returns:
      - channels: list of channel names
      - dist_matrix: numpy array of shape (n_channels, n_channels)
    """
    channels = list(channel_users.keys())
    n = len(channels)
    dist_matrix = np.zeros((n, n))
    for i in range(n):
        for j in range(i, n):
            dist = distance_func(channel_users[channels[i]], channel_users[channels[j]], **kwargs)
            dist_matrix[i, j] = dist
            dist_matrix[j, i] = dist  # symmetric matrix
    return channels, dist_matrix

def project_channels_to_3d(dist_matrix, random_state=42):
    """
    Uses Multidimensional Scaling (MDS) to project channels (based on the distance matrix)
    into 3 dimensions.
    """
    mds = MDS(n_components=3, dissimilarity='precomputed', random_state=random_state)
    coords = mds.fit_transform(dist_matrix)
    return coords

def plot_channels_3d(channels, coords):
    """
    Plots channels in 3D space using matplotlib.
    Each channel is labeled with its name.
    """
    fig = plt.figure(figsize=(10, 8))
    ax = fig.add_subplot(111, projection="3d")
    xs, ys, zs = coords[:, 0], coords[:, 1], coords[:, 2]
    ax.scatter(xs, ys, zs, c="blue", marker="o", s=50)

    # Annotate each point with the channel name
    for i, channel in enumerate(channels):
        ax.text(xs[i], ys[i], zs[i], channel, size=9, zorder=1, color="red")

    ax.set_title("3D Projection of Twitch Channels by Shared Chat Users")
    ax.set_xlabel("X")
    ax.set_ylabel("Y")
    ax.set_zlabel("Z")
    plt.tight_layout()
    plt.show()

def main():
    # Step 1: Get the mapping: channel -> set of users
    channel_users = get_channel_user_sets()
    if not channel_users:
        print("No channel data found in the database.")
        return

    print(f"Retrieved user data for {len(channel_users)} channels.")

    # Step 2: Compute pairwise distance matrix using the exponential transformation.
    # Adjust k to control the effect of any overlap.
    channels, dist_matrix = compute_distance_matrix(channel_users, custom_distance_exponential, k=100)
    print(dist_matrix)
    print("Computed distance matrix using exponential transformation.")

    # Step 3: Use MDS to project the distance matrix to 3D coordinates
    coords = project_channels_to_3d(dist_matrix)
    print("Computed 3D coordinates using MDS.")

    # Step 4: Plot the 3D coordinates
    plot_channels_3d(channels, coords)

if __name__ == "__main__":
    main()
