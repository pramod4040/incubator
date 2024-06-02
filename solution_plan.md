# Proposed Approach

Let's devise a method to determine a MATCH SCORE for each school, with the highest score indicating the best match.

## How to Calculate the MATCH SCORE:
We can explore various options for this:

- Initially, I've implemented a character-matching algorithm between the English and Nepali names of schools. This was done using the `calculate_similarity` function.
  
- Additionally, we can extend this approach to include other fields such as district and apply weighting to compute an overall MATCH SCORE between schools.

## Outcome
The results will be stored in the file `highest_scores_6k.csv`.

## Assistance Needed With
- Generating the Velthuis transliteration for the 'district1' column in the `school_list_A.tsv` file.
