# SOURCE 1: https://www.youtube.com/watch?v=WGlMlS_Yydk (start @ 8:00)
# SOURCE 2: https://www.edureka.co/blog/apriori-algorithm/
# SOURCE 3: https://journalofbigdata.springeropen.com/articles/10.1186/s40537-021-00473-3#Sec9 

# TODO AT END: 
# 1) Move everything into a function or class
# 2) Change dataset, minsup, minconf, k (parameter of MLKP), threshold into parameters


# ************************
# STEP 1

# TODO AT END: Implement threshold and alternate direct generation method (will do this later if necessary)

import numpy as np
import pandas as pd

MIN_SUPPORT = 0.3
DATASET_FILE_NAME = 'test2.csv'
NUM_PARTITIONS = 3

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

# 'reference' contains key-value pairs of the form a : i, where a is some item name and i is a non-negative integer. This allows us to transform item names into non-negative integers which is required by later part_graph step. This allow us to access the actual item names later

integer_to_item = {}
item_to_integer = {}

i = 0
for item in itemsets_1:
  integer_to_item[i] = item
  item_to_integer[item] = i
  i += 1

# 'itemsets_2_modified' replaces each key-value pair (a, b): w of 'itemsets_2' with (i, j) : w, where i and j are distinct non-negative integers

  
# 2 itemsets above the min sup threshold
itemsets_2 = {}
itemsets_2_modified = {}

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

for key in itemsets_2.keys():
  itemsets_2_modified[(item_to_integer[key[0]], item_to_integer[key[1]])] = itemsets_2[key]

# TODO (DONE): An adjustment needs to be made here to accomodate the PyMetis part_graph function. 

# TODO (DONE): Mistake in this code. "itemsets_2_modified" should be in the form (i, j) : w, where i and j are distinct non-negative integers.
# Further, "reference" should be in the form a : i. Possibly change name of "reference" to 


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

# TOOD: IAG must be completely changed. We need 3 data structures instead: adjncy, xadj, eweights. All 3 are initialized as lists and should become Numpy arrays. 


# IAG is obsolete and incompatible with pymetis part_graph function

IAG = {}
for key in itemsets_2_modified:
    IAG[key[0]] = IAG.get(key[0], []) + [(key[1], itemsets_2_modified[key])]
    IAG[key[1]] = IAG.get(key[1], []) + [(key[0], itemsets_2_modified[key])]

adjncy = []
xadj = []
eweights = []

index = 0
xadj.append(index)

for i in range(len(IAG.keys())):
  if i in IAG.keys():
    xadj.append(index + len(IAG[i]))
    index += len(IAG[i]) + 1
    for j in range(len(IAG[i])):
      j -= 1
      adjncy.append(IAG[i][j][0])
      weight = int(IAG[i][j][1] * df.shape[0])
      eweights.append(weight)

import ctypes

c_adjncy_type =  ctypes.c_int * len(adjncy)
c_xadj_type = ctypes.c_int * len(xadj)
c_eweights_type = ctypes.c_int * len(eweights)

c_adjncy = c_adjncy_type()
c_xadj = c_xadj_type()
c_eweights = c_eweights_type()

for i in range(len(adjncy)):
  c_adjncy[i] = adjncy[i]

for i in range(len(xadj)):
  c_xadj[i] = xadj[i]

for i in range(len(eweights)):
  c_eweights[i] = eweights[i]
      
# ************************
# STEP 3

# IAG is of the form 
# {
#    0: [(1, 3), (2, 2)],
#    1: [(0, 3), (2, 5)],
#    2: [(0, 2), (1, 5), (3, 1)],
#    3: [(2, 1)]
# }
# where 0, 1, 2, 3 are items and the numbers represent support levels.

#!pip install pymetis
import pymetis

n_cuts, membership = pymetis.part_graph(NUM_PARTITIONS, xadj = c_xadj, adjncy = c_adjncy, eweights = c_eweights)

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

# Redefined "list_partition" to "IAG_partition" for greater clarity

IAG_partitions = [[] * NUM_PARTITIONS]
# list_partitions is a list of lists with the 0th list being the 0th cluster and iterrows
# contains all the nodes in that cluster and so on

for i in range(len(membership)):
  IAG_partitions[membership[i]].append(i)

# Change entries in "IAG_partitions" back to item names for easy comparison in the transaction intersection step

for i in range(len(IAG_partitions)):
  for j in range(len(partition)):
    k = IAG_partitions[i][j]
    IAG_partitions[i][j] = integer_to_item[k];

dataset_partitions = []
# dataset_partition is a list of dataframes with the ith dataframe corresponding to a list of transactions that intersect with the ith partition in list_partitions. These intersections will themselves be represented by lists.

# The output from this step will be a Pandas DataFrame, because I assume this will be the easiest to directly plug into Apriori or FP growth in the next step

# TODO: I'm not sure if this is the best approach. Nesting four for loops together does not seem very efficient. See if there's a better approach.

for i in range(NUM_PARTITIONS):
  dataset_partition = []
  for index, row in df.iterrows():
    list = []
    for item1 in row:
      for item2 in IAG_partitions[i]:
        if item1 == item2:
          list.append[item1]
          break
    if len(list) > 2:
      dataset_partition.append(list)
  df_i = pd.DataFrame(dataset_partition)
  if not df_i.empty:
    dataset_partitions.append(df_i)    

# ************************
# STEP 5

# Apply apriori or FP-growth to each partition in "dataset_partitions"

# ************************
# STEP 6

# ************************
# STEP 7

# ************************