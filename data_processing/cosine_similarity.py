import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from sklearn.manifold import MDS
from sklearn.metrics.pairwise import cosine_similarity


def _compute_weighted_cosine_similarity(group_data: dict) -> np.ndarray:
    unique_people = sorted(set(name for group in group_data.values() for name in group))
    num_people = len(unique_people)

    group_vectors = []
    group_names = list(group_data.keys())

    for group in group_names:
        vector = [group_data[group].get(person, 0) for person in unique_people]
        group_vectors.append(vector)

    group_vectors = np.array(group_vectors)

    weight_matrix = np.zeros((len(group_vectors), len(group_vectors)))

    for i in range(len(group_vectors)):
        for j in range(i, len(group_vectors)):
            overlap = np.minimum(group_vectors[i], group_vectors[j])
            weight = np.sum(overlap)
            weight_matrix[i, j] = weight_matrix[j, i] = weight

    similarity_matrix = cosine_similarity(group_vectors)
    
    similarity_matrix *= (1 + weight_matrix / np.max(weight_matrix))
    
    return similarity_matrix


def compute_3d_cords(group_data: dict) -> pd.DataFrame:
    similarity_matrix: np.ndarray = _compute_weighted_cosine_similarity(group_data)

    distance_matrix = 1 - similarity_matrix
    mds = MDS(n_components=3, dissimilarity="precomputed", random_state=42)
    embedding_3d = mds.fit_transform(distance_matrix)

    df_3d = pd.DataFrame(embedding_3d, columns=["X", "Y", "Z"])
    df_3d["Group"] = list(group_data.keys())
    return df_3d


def _plot_similarity_matplot(cords_df: pd.DataFrame) -> None:
    fig = plt.figure(figsize=(8, 6))
    ax = fig.add_subplot(111, projection='3d')

    ax.scatter(cords_df["X"], cords_df["Y"], cords_df["Z"], c='blue', s=50)

    for i, row in cords_df.iterrows():
        ax.text(row["X"], row["Y"], row["Z"], row["Group"], fontsize=12)

    ax.set_xlabel("X Axis")
    ax.set_ylabel("Y Axis")
    ax.set_zlabel("Z Axis")
    ax.set_title("Group Relationships in 3D using Weighted Cosine Similarity")

    plt.show()


# group_data_b = {
#     "Group A": {"Alice": 5, "Bob": 3, "Charlie": 2, "David": 0, "Eve": 0, "Frank": 0},
#     "Group B": {"Alice": 2, "Bob": 0, "Charlie": 4, "David": 3, "Eve": 0, "Frank": 0},
#     "Group C": {"Alice": 0, "Bob": 5, "Charlie": 0, "David": 1, "Eve": 4, "Frank": 0},
#     "Group D": {"Alice": 0, "Bob": 0, "Charlie": 3, "David": 0, "Eve": 2, "Frank": 5},
#     "Group E": {"Alice": 4, "Bob": 0, "Charlie": 0, "David": 5, "Eve": 0, "Frank": 3},
# }

# cords_3d: pd.DataFrame = compute_3d_cords(group_data_b)
# print(cords_3d)
# _plot_similarity_matplot(cords_3d)
