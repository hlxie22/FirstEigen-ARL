#################################
# IMPORTS

import pandas as pd
import numpy as np
from mlxtend.frequent_patterns import fpgrowth
from mlxtend.frequent_patterns import apriori
from mlxtend.preprocessing import TransactionEncoder
from mlxtend.frequent_patterns import association_rules
import networkx as nx
import metis
import time

#################################
# GLOBAL CONSTS

FILE_NAME = 'testWithPaperData.csv'
MIN_SUP = 0.1
MIN_CONF = 0.7
NUM_PARTS = 2
NUM_CUTS = 2
THRESHOLD = 20

#################################
# DATA READING AND PRE-PROCESSING

df = pd.read_csv(FILE_NAME)
df_dtypes = df.dtypes
df_cols = df.columns

num = pd.DataFrame()


for col in df_cols:
    if np.issubdtype(df_dtypes[col], np.number):
        num[col] = df.pop(col)
cat = df