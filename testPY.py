# SOURCE (FP-Growth): http://rasbt.github.io/mlxtend/user_guide/frequent_patterns/fpgrowth/



# *******************************
# STEP 1

# also try using the apyori library

import pandas as pd
import numpy as np
from mlxtend.frequent_patterns import fpgrowth
from mlxtend.frequent_patterns import apriori

FILE_NAME = 'test2.csv'
MIN_SUP = 0.1
MIN_CONF = 0.7

def csvToOneHotBoolDF(csv_file_name):
    df = pd.read_csv(csv_file_name)
    for col in df:
        df[col] = df[col].map({col: True})
    df.fillna(False, inplace=True)
    return df

df = csvToOneHotBoolDF(FILE_NAME)

# Note: now dataset is a pandas DataFrame where each col represents a different item
# and each value is boolean (True if the item is in that particular transaction (where each transaction is represented by each row) and False otherwise)

freq_itemsets = fpgrowth(df, min_support=MIN_SUP, use_colnames=True)
frequent_itemsets['length'] = frequent_itemsets['itemsets'].apply(lambda x: len(x))

print(freq_itemsets)

'''
# or alternatively fpgrowth(df, min_support=MIN_SUPPORT, use_colnames=True)
# to preserve the col names instead of using the col index

# variable1 is a pandas DataFrame with the itemsets of support >= MIN_SUPPORT
# we then have to filter those itemsets to only ones with 1 or 2 elements






# *******************************
# STEP 2

import metis


#                3
#           0 ------- 1
#            \       /
#          2  \     / 5
#              \   /
#               \ /     1
#                2 ----------- 3
# (I just randomly ordered the vertices by randomly assigning a non-negative integer to each vertex and started at 0 and then 1 and 2 and so on):
# the above graph would be represented as such:
#
# adjlist = [((1, 3), (2, 2)), ((0, 3), (2, 5)), ((0, 2), (1, 5), (3, 1)), ((2, 1))]
# adjlist is a list of tuples where the tuple at index i represents node i
# each element of each tuple is a 2-tuple where it is of the form (index, weight)
#
# test this:


adjlist = [
    ((2, 3), (3, 2), (4, 2), (5, 2)), # represents node 1
    ((1, 3), (3, 3)), # represents node 2
    ((1, 2), (2, 3)), # represents node 3
    ((1, 2), (5, 3)), # represents node 4
    ((1, 2), (4, 3)) # represents node 5
]
graph = metis.adjlist_to_metis(adjlist)

# *******************************
# STEP 3
K = 2
objval, parts = metis.part_graph(graph, nparts=K)
print(parts)

# we don't care about objval, we just want parts
# parts is a list of partition indices corresponding
# (i.e. (I think) parts = [0, 0, 1, 4, 0, 2, 2, 1, ...] if index1 (the first element of adjlist)
# is in partition 0, and index2 (the second element of adjlist) is in partition 0, and so on.
# I think in general parts[i] = the partition in which adjlist[i] lies)
# expected output: parts = [0, 0, 0, 1, 1]
# or something of that form, we just need nodes 1, 2, and 3 to be a part of the same group
# and 4 and 5 to be a part of another group

# *******************************
# STEP 4

parts_transformed = [set() for i in range(K)]
for i in range(len(parts)):
    parts_transformed[parts[i]].add(i)

# Turn dataset into list of sets (call that new_dataset)
# Check if each set in new_dataset is a subset of parts_transformed

dataset_partitions = []

# *******************************
# STEP 5

# run the same algorithm frome the same library as STEP 1
# (obviously there has to be some changes to the functions called and the data being passed)


# *******************************
# STEP 6

# find the union of the results from STEP 5

print('DONE')
'''