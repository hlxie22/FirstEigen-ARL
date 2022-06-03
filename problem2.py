# SOURCE 1: https://www.youtube.com/watch?v=WGlMlS_Yydk (start @ 8:00)
# SOURCE 2: https://www.edureka.co/blog/apriori-algorithm/
# SOURCE 3: https://journalofbigdata.springeropen.com/articles/10.1186/s40537-021-00473-3#Sec9 

# TODO AT END: 
# 1) Move everything into a function or class
# 2) Change dataset, minsup, minconf, k (parameter of MLKP), threshold into parameters


# ************************
# STEP 1

# TODO: Implement threshold and alternate direct generation method

import numpy as np
import pandas as pd

MIN_SUPPORT = 0.3
DATASET_FILE_NAME = 'test2.csv'

# data format: each row is a seperate transaction, and each col is an item (see test2.csv for example)
df = pd.read_csv(DATASET_FILE_NAME)
itemsets_1 = {item:0 for item in df}

# 1-itemsets above the min sup threshold
for i in df:
    for j in df[i]:
        if j == i:
            itemsets_1[i] += 1
    itemsets_1[i] /= df.shape[0]
    if itemsets_1[i] < MIN_SUPPORT:
        del itemsets_1[i]

# 2 itemsets above the min sup threshold
itemsets_2 = {}
keys_list = list(itemsets_1.keys())
for i in range(len(keys_list)):
    for j in range(i+1, len(keys_list)):
        itemsets_2[(keys_list[i], keys_list[j])] = 0

for row in df.values:
    for key in itemsets_2:
        if (key[0] in row) and (key[1] in row):
            itemsets_2[key] += 1

keys_list = list(itemsets_2.keys())
for i in keys_list:
    itemsets_2[i] /= df.shape[0]
    if itemsets_2[i] < MIN_SUPPORT:
        del itemsets_2[i]



# NOTE: An adjustment needs to be made here to accomodate the PyMetis part_graph function. 


# 'itemsets_2_modified' replaces each key-value pair (a, b): w of 'itemsets_2' with i : w, where i is some non-negative integer
# 'reference' contains key-value pairs of the form (a, b) : i, which allow us to access the actual item pairs later

reference = {}
i = 0
itemsets_2_modified = {}
for key in itemsets_2:
  reference[i] = key
  itemsets_2_modified[i] = itemsets_2[key]
  i += 1


### TODO (DONE)
# iterate over rows of df
# for each row check all of the 2-tuples in 'itemsets_2'
# see if each one is in that row of df
# if it is, then update the corresponding value in 'itemsets_2' by incrementing 'itemsets_2[i]' by 1
# (for whichever 'i' it is)



# ************************
# STEP 2

### TODO
# (sidenote) store graph data as an adjacency list (as opposed to an adjacency matrix)
# (continuing from above) example:      
#                3
#           A ------- B
#            \       /
#          2  \     / 5
#              \   /
#               \ /     1
#                C ----------- D
# the above graph would be represented as such:
# {
#    A: [(B, 3), (C, 2)],
#    B: [(A, 3), (C, 5)],
#    C: [(A, 2), (B, 5), (D, 1)],
#    D: [(C, 1)]
# }
# iterate over the keys from step 1
# (we only care about the 2-itemsets (we created the 1-itemsets only to help with creating the 2-itemsets))
# (each element of 'itemsets_2' is of the form (a, b): w, where a and b are 2 distinct items and w is the corresponding weight)
# for element of 'itemsets_2', add 2 things to the adjacency list representation of the IAG:
# 1. a: [(b, w)]
# 2. b: [(a, w)]

# This step is adjusted to operate on 'itemsets_2_modified'

IAG = {}
for key in itemsets_2_modified:
    IAG[key[0]] = IAG.get(key[0], []) + [(key[1], itemsets_2_modified[key])]
    IAG[key[1]] = IAG.get(key[1], []) + [(key[0], itemsets_2_modified[key])]

# ************************
# STEP 3

# IAG is of the form 
# {
#    A: [(B, 3), (C, 2)],
#    B: [(A, 3), (C, 5)],
#    C: [(A, 2), (B, 5), (D, 1)],
#    D: [(C, 1)]
# }
# where A, B, C, D are items and the numbers represent support levels.

!pip install pymetis
import pymetis

n_cuts, membership = pymetis.part_graph(3, adjacency = IAG)

# n_cuts is the number of cuts that the algorithm made to edges of the graph. This number is not important
# membership is represented as something like [2, 1, 1, 0]. In this case, it means that 

# A few potential issues:
# It is possible that both Python wrapper candidates do not output the MLKP specifically, instead a simpler partitioning algorithm that may not be as fast.
# We may need to come back and re-evaluate this if it becomes a problem. Even if we are not able to use MLKP, in particular, the other METIS partitioning algorithms are all very fast as well. 
# Potential possibilities for MLKP implementation:
# 1: Python wrapper, metis or pymetis
# 2: Directly link METIS, create our own wrapper somehow
# 3: Directly code the MLKP algorithm on our own: http://glaros.dtc.umn.edu/gkhome/node/107 

# ************************
# STEP 4

# TODO: Complete rest of implementation


for index, row in df.iterrows():


# ************************
# STEP 5

# ************************
# STEP 6

# ************************
# STEP 7

# ************************