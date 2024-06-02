from collections import Counter
import csv
import numpy as np
import pandas as pd
import multiprocessing


def calculate_similarity(phrase1, phrase2):
    # Normalize the phrases by converting to lowercase
    phrase1 = phrase1.lower()
    phrase2 = phrase2.lower()

    # Remove spaces and punctuation for a direct character comparison
    phrase1 = ''.join(e for e in phrase1 if e.isalnum())
    phrase2 = ''.join(e for e in phrase2 if e.isalnum())

    # Count the frequency of each character in both phrases
    counter1 = Counter(phrase1)
    counter2 = Counter(phrase2)

    # Find the common characters
    common_characters = counter1 & counter2

    # Count the total number of common characters
    num_common_characters = sum(common_characters.values())

    # Calculate the average length of the two phrases
    average_length = (len(phrase1) + len(phrase2)) / 2

    # Calculate the similarity score
    similarity_score = num_common_characters / average_length
    
    return similarity_score

# Example usage
# phrase1 = "mitrataa maa bi"
# phrase2 = "Mitrata Ma. Vi"
# similarity_score = calculate_similarity(phrase1, phrase2)
# similarity_score



def read_tsv(file_path):
    # Initialize an empty list to store the rows
    data = []
    
    # Open the TSV file
    with open(file_path, mode='r', newline='', encoding='utf-8') as file:
        # Create a CSV reader object specifying the delimiter as tab
        reader = csv.reader(file, delimiter='\t')

         # Skip the first row (header)
        next(reader)

        # Iterate over each row in the TSV file
        for row in reader:
            # Append the row to the data list
            if (len(data) > 6000):
                break
            data.append(row)
            
    return data


file_path_A = '2024-05_school_mapping/data/school_list_A.tsv'
school_list_A = read_tsv(file_path_A)


file_path_B = '2024-05_school_mapping/data/school_list_B.tsv'
school_list_B = read_tsv(file_path_B)



# # Extract IDs and names
school_a_ids = [row[0] for row in school_list_A]
school_b_ids = [row[0] for row in school_list_B]


# Create a matrix to store the similarity scores
similarity_matrix = np.zeros((len(school_a_ids), len(school_b_ids)))


# Compute the similarity scores using dynamic programming

# for i, school_a_data in enumerate(school_list_A):
#     for j, school_b_data in enumerate(school_list_B):
#         similarity_matrix[i, j] = calculate_similarity(school_a_data[2], school_b_data[1])


# PARALLEL START

def calculate_similarity_parallel(i, school_a_data, school_list_B):
    similarities = []
    for j, school_b_data in enumerate(school_list_B):
        similarity = calculate_similarity(school_a_data[2], school_b_data[1])
        similarities.append(similarity)
    return similarities


if __name__ == "__main__":
    num_processes = multiprocessing.cpu_count()  # Number of available CPU cores
    pool = multiprocessing.Pool(processes=num_processes)

    results = []
    for i, school_a_data in enumerate(school_list_A):
        results.append(pool.apply_async(calculate_similarity_parallel, args=(i, school_a_data, school_list_B)))

    # Wait for all processes to finish
    pool.close()
    pool.join()

     # Retrieve and update results in similarity_matrix
    for k, result in enumerate(results):
        similarities = result.get()
        similarity_matrix[k, :] = similarities  # Append similarities[1:]  # Rest are similarities for index i


    # Find the indices of the highest scores in each row
    highest_indices = np.argmax(similarity_matrix, axis=1)

    similarity_df = pd.DataFrame(similarity_matrix, index=school_a_ids, columns=school_b_ids)


    # Create a DataFrame to show the highest similarity score and corresponding school_b_id for each school_a_id
    highest_scores_df = pd.DataFrame({
        'school_a_id': school_a_ids,
        'highest_score': similarity_matrix[np.arange(len(school_a_ids)), highest_indices],
        'school_b_id': [school_b_ids[idx] for idx in highest_indices]
    })


    # Save the similarity matrix to a file if needed
    similarity_df.to_csv('similarity_matrix.csv')

    highest_scores_df.to_csv('highest_scores.csv')

    # Display the similarity matrix
    print(highest_scores_df)


# PARALLEL END       




# # Find the indices of the highest scores in each row
# highest_indices = np.argmax(similarity_matrix, axis=1)

# similarity_df = pd.DataFrame(similarity_matrix, index=school_a_ids, columns=school_b_ids)


# # Create a DataFrame to show the highest similarity score and corresponding school_b_id for each school_a_id
# highest_scores_df = pd.DataFrame({
#     'school_a_id': school_a_ids,
#     'highest_score': similarity_matrix[np.arange(len(school_a_ids)), highest_indices],
#     'school_b_id': [school_b_ids[idx] for idx in highest_indices]
# })


# # Save the similarity matrix to a file if needed
# similarity_df.to_csv('similarity_matrix.csv')

# highest_scores_df.to_csv('highest_scores.csv')

# # Display the similarity matrix
# print(highest_scores_df)


