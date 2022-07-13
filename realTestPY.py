import pandas as pd
import numpy as np
from mlxtend.frequent_patterns import fpgrowth
from mlxtend.frequent_patterns import apriori
from mlxtend.preprocessing import TransactionEncoder
import networkx as nx
import time


import matplotlib.pyplot as plt
import networkx as nx
import metis

import pymetis

df = pd.DataFrame([
    ['2012-05-14', 'a1', '1', 'a2', 'a3', '11'],
    ['2012-05-15', 'b1', '2', 'b2', 'b3', '12'],
    ['2012-05-16', 'c1', '3', 'c2'],
    ['2012-05-17', 'd1', '4', 'd2', 'd3', '13'],
    ['2012-05-18', 'e1', '5', 'e2', 'e3', '14'],
    ['2012-05-19', 'f1'],
])

#print(df.select_dtypes(include='int'))

print(df.select_dtypes(include='number'))
print(df.select_dtypes(include='object'))
print(df.select_dtypes(include='datetime'))