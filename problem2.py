# SOURCE 1: https://www.youtube.com/watch?v=WGlMlS_Yydk (start @ 8:00)
# SOURCE 2: https://www.edureka.co/blog/apriori-algorithm/

# ************************
# STEP 1

import numpy as np
import pandas as pd

MIN_SUPPORT = 0.7
DATASET_FILE_NAME = 'test2.csv'

# data format: each row is a seperate transaction, and each col is an item (see test2.csv for example)
df = pd.read_csv(DATASET_FILE_NAME)
itemsets_1 = {item:0 for item in df}

# 1 itemsets above the min sup threshold
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


# ************************
# STEP 2

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