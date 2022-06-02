# SOURCE 1: https://www.youtube.com/watch?v=WGlMlS_Yydk (start @ 8:00)
# SOURCE 2: https://www.edureka.co/blog/apriori-algorithm/

# ************************
# STEP 1 (DONE)

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

### TODO (DONE)
# iterate over rows of df
# for each row check all of the 2-tuples in 'itemsets_2'
# see if each one is in that row of df
# if it is, then update the corresponding value in 'itemsets_2' by incrementing 'itemsets_2[i]' by 1
# (for whichever 'i' it is)


# ************************
# STEP 2 (DONE)

### TODO (DONE)
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

IAG = {}
for key in itemsets_2:
    IAG[key[0]] = IAG.get(key[0], []) + [(key[1], itemsets_2[key])]
    IAG[key[1]] = IAG.get(key[1], []) + [(key[0], itemsets_2[key])]

# ************************
# STEP 3

# ************************
# STEP 4

# ************************
# STEP 5

# ************************
# STEP 6

# ************************
# STEP 7

# ************************