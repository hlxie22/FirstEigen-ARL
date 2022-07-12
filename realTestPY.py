import pandas as pd
import numpy as np
from mlxtend.frequent_patterns import fpgrowth
from mlxtend.frequent_patterns import apriori
from mlxtend.preprocessing import TransactionEncoder
import networkx as nx
import time


import matplotlib.pyplot as plt
import networkx as nx



df = pd.DataFrame([[1,2,3,4.0001],[5,-6,-7.3210]], columns=['a', 'b','c','d'])#,[7,8,'yelp'],['a','b','c']])

print(df.columns)
for i in df.columns:
    print(i)