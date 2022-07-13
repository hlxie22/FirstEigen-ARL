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

''' Works! YAY! PyMetis example (same as research paper so we can check results)
nparts = 2

'''
'''
adjncy = [
    1,2,3,4, # node 0
    0,2,     # node 1
    0,1,     # node 2
    0,4,     # node 3
    0,3      # node 4
]

xadj = [0,4,6,8,10,12] # maybe add 12 to the end?

eweights = [
    3,2,2,2, # node 0
    3,3,     # node 1
    2,3,     # node 2
    2,3,     # node 3
    2,3      # node 4
]

cutcount, part_vert = pymetis.part_graph(nparts, xadj=xadj, adjncy=adjncy, eweights=eweights)
print(part_vert)
'''

df = pd.DataFrame([
    [3, (0,1)],
    [2, (0,2)],
    [2, (0,3)],
    [2, (0,4)],
    [3, (1,2)],
    [3, (3,4)]
], columns=['support', 'itemsets'])