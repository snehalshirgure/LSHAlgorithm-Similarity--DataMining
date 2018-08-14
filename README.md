# LSHAlgorithm-Similarity--DataMining

# Project Overview

In this task, you will need to develop the LSH technique using the ml-latest-small/ratings.csv file from MovieLens dataset(available online). 
The goal of this task is to find similar movies according to the ratings of the users. 
In this problem, we will focus on 0-1 ratings rather than the actual ratings of the users. To be more specific, if a user has rated a movie, then his contribution to the characteristic matrix is 1 while if he hasn't rated a movie his contribution
is 0. Our goal is to identify similar movies whose Similarity is greater or equal to 0.5.

# Jaccard based LSH 
Implementation Guidelines - Approach
The original characteristic matrix must be of size [users] x [movies]. Each
cell contains a 0 or 1 value depending on whether the user has rated the movie
or not. Once the matrix is built, you are free to use any collection of hash
functions that you think would result in a more consistent permutation of the
row entries of the characteristic matrix.
Some potential hash functions could be of type:
f(x) = (ax + b)%m
or
f(x) = ((ax + b)%p)%m
where p is any prime number and m is the number of bins.
You can use any value for the a, b, p or m parameters of your
implementation.
Once you have computed all the required hash values, you must build the
Signature Matrix. Once the Signature Matrix is built, you must divide the
Matrix into b bands with r rows each, where bands x rows = n (n is the number
of hash functions), in order to generate the candidate pairs. Remember that in
order for two movies to be a candidate pair their signature must agree (i.e., be
identical) with at least one band.
Once you have computed the candidate pairs, your final result will be the
candidate pairs whose Jaccard Similarity is greater than or equal to 0.5. Af-
ter computing the final similar items, you must compare your results against
the provided ground truth dataset using the precision and recall metrics. The
ground truth dataset contains all the movies pairs that have Jaccard similarity
above or equal to 0.5. 

For Similarity >= 0.5 compute the precision and recall for the similar
movies that your algorithm has generated against the ground truth data file.

# Results

Precision: 1.0
Recall: 0.982997295838
Time taken: 196.178999901 sec




