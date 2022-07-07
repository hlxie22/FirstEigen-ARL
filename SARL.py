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

#################################
# SARL CLASS

class SARL:

    #################################
    # CONSTRUCTOR

    def __init__(self, file_name, min_sup, min_conf, num_parts, num_cuts):
        self.df = self.get_data(file_name)
        self.min_sup = min_sup
        self.min_conf = min_conf
        self.num_parts = num_parts
        self.num_cuts = num_cuts

    #################################
    # GET DATA

    def get_data(self, file_name):
        df = pd.read_csv(file_name, dtype=str)
        for col in df:
            df[col] = df[col].map({col: True})
        df.fillna(False, inplace=True)
        return df

    #################################
    # STEP 1: Find freq 2-itemsets using Apriori (DONE)

    def step_1(self):
        freq_itemsets = apriori(self.df, min_support=self.min_sup)
        freq_itemsets['length'] = freq_itemsets['itemsets'].apply(lambda x: len(x))
        freq_itemsets = freq_itemsets[freq_itemsets['length'] == 2]
        freq_itemsets.drop('length', axis=1, inplace=True)
        freq_itemsets['support'] = (freq_itemsets['support'] * self.df.shape[0]).astype(int)
        return freq_itemsets

    #################################
    # STEP 2: Construct IAG (DONE)

    def step_2(self):
        freq_itemsets = self.step_1()
        iag = nx.Graph()
        for row in freq_itemsets.values:
            edge = tuple(row[1])
            iag.add_edge(edge[0], edge[1], weight=row[0])
        return iag

    #################################
    # STEP 3: Partition IAG using MLkP (DONE)

    def step_3(self):
        iag = self.step_2()
        obj_val, parts = metis.part_graph(iag, nparts=self.num_parts, ncuts=self.num_cuts)
        return parts

    #################################
    # STEP 4: Partition dataset according to STEP 3 (DONE)

    def step_4(self):
        parts = self.step_3()
        parts_modified = [set() for i in range(self.num_parts)]
        dataset_modified = []
        dataset_partitions = [[] for i in range(self.num_parts)]
        for i in range(len(parts)):
            parts_modified[parts[i]].add(i)
        for i in range(self.df.shape[0]):
            dataset_modified.append(set(np.arange(self.df.shape[1])[self.df.values[i]]))
        for i in dataset_modified:
            for j in range(self.num_parts):
                if i.issubset(parts_modified[j]):
                    dataset_partitions[j].append(list(i))
                    break
        return dataset_partitions

    #################################
    # STEP 5: Mine freq itemsets on each partition using modified Apriori or FP-Growth (DONE)

    def step_5(self):
        dataset_partitions = self.step_4()
        trxn_ncoder = TransactionEncoder()
        result = []
        for i in dataset_partitions:
            trxn_ncoder_arry = trxn_ncoder.fit(i).transform(i)
            df = pd.DataFrame(trxn_ncoder_arry, columns=trxn_ncoder.columns_)
            freq_itemsets_trxn_part = fpgrowth(df, min_support=self.min_sup)
            result.append(freq_itemsets_trxn_part)
        return result

    #################################
    # STEP 6: Find union of results from each partition (STEP 5) (DONE)

    def step_6(self):
        result = self.step_5()
        union = pd.DataFrame()
        for i in result:
            union = pd.concat([union, i], ignore_index=True)
        union['length'] = union['itemsets'].apply(lambda x: len(x))
        union = union[union['length'] > 2]
        union.drop('length', axis=1, inplace=True)
        return union

    #################################
    # STEP 7: Generate association rules using Apriori-ap-genrules on freq itemsets (STEP 6) (DONE)
    def step_7(self):
        union = self.step_6()
        rules = association_rules(union, metric='confidence', min_threshold=self.min_conf)
        return rules


test = SARL(FILE_NAME, MIN_SUP, MIN_CONF, NUM_PARTS, NUM_CUTS)
start = time.time()
a = test.step_7()
end = time.time()
print(a)
#print(end-start)


'''
start = time.time()
df = pd.read_csv(FILE_NAME, dtype=str)
for col in df:
    df[col] = df[col].map({col: True})
df.fillna(False, inplace=True)
a = fpgrowth(df, min_support=MIN_SUP)
end = time.time()
print(a)
print(end-start)
'''