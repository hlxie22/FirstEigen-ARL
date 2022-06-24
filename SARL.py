#################################
# IMPORTS

import pandas as pd
import numpy as np
from mlxtend.frequent_patterns import fpgrowth
from mlxtend.frequent_patterns import apriori
from mlxtend.preprocessing import TransactionEncoder
import metis

#################################
# GLOBAL CONSTS

FILE_NAME = 'test2.csv'
MIN_SUP = 0.1
MIN_CONF = 0.7
NUM_PARTS = 2

#################################
# SARL CLASS

class SARL:

    #################################
    # CONSTRUCTOR

    def __init__(self, file_name, min_sup, num_parts, num_cuts):
        self.df = self.get_data(file_name)
        self.min_sup = min_sup
        self.num_parts = num_parts
        self.num_cuts = num_cuts

    #################################
    # GET DATA

    def get_data(self, file_name):
        df = pd.read_csv(file_name)
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
        freq_itemsets['support'] = int(freq_itemsets['support'] * self.df.shape[0])
        return freq_itemsets

    #################################
    # STEP 2: Construct IAG

    def step_2(self):
        freq_itemsets = self.step_1()
        nodes = pd.DataFrame(freq_itemsets.pop('itemsets'))
        nodes[[0, 1]] = pd.DataFrame(nodes['itemsets'].tolist(), index=nodes.index)
        nodes.drop('itemsets', axis=1, inplace=True)
        nodes = nodes.values
        freq_itemsets = freq_itemsets.values
        num_nodes = np.amax(nodes)
        adj_list = [[] for i in range(num_nodes)]
        for node in range(num_nodes):
            where = np.array(np.where(nodes == node))
            where[1] = (where[1] + 1) % 2
            where = where.T
            for row, col in where:
                adj_list[node].append((nodes[row][col], freq_itemsets[row][0]))
            adj_list[node] = tuple(adj_list[node])
        iag = metis.adjlist_to_metis(adj_list)
        return iag

    #################################
    # STEP 3: Partition IAG using MLkP

    def step_3(self):
        iag = self.step_2()
        obj_val, parts = metis.part_graph(iag, nparts=self.num_parts, ncuts=self.num_cuts)
        return parts

    #################################
    # STEP 4: Partition dataset according to STEP 3

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
                if i.issubet(parts_modified[j]):
                    dataset_partitions[j].append(list(i))
                    break
        return dataset_partitions

    #################################
    # STEP 5: Mine freq itemsets on each partition using modified Apriori or FP-Growth

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
    # STEP 6: Find union of results from each partition (STEP 5)

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
    # STEP 7: Generate association rules using Apriori-ap-genrules on freq itemsets (STEP 6)