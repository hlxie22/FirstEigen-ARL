import pandas as pd
import numpy as np
from mlxtend.frequent_patterns import fpgrowth
from mlxtend.frequent_patterns import apriori
from mlxtend.preprocessing import TransactionEncoder
import networkx as nx
import time


import matplotlib.pyplot as plt
import networkx as nx


df = pd.DataFrame([[1,2,3],[1,3,3],[2,2,2],[2,4,5]], columns=['a','b','c'])
print(df)
print()
print(df.min())
print(df.min()['c'])