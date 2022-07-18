#################################
# IMPORTS

import numpy as np
import pandas as pd
from mlxtend.frequent_patterns import fpgrowth
from mlxtend.frequent_patterns import apriori
from mlxtend.preprocessing import TransactionEncoder
from mlxtend.frequent_patterns import association_rules
import pymetis

#################################
# SARL CLASS

class SARL:

    #################################
    # CONSTRUCTOR

    def __init__(self, df, min_sup, min_conf, num_parts):
        self.df = df
        self.min_sup = min_sup
        self.min_conf = min_conf
        self.num_parts = num_parts

    #################################
    # STEP 1: Find freq 2-itemsets using Apriori (TO-DO: OPTIMIZE)

    def step_1(self):
        freq_itemsets_1_2 = apriori(self.df, min_support=self.min_sup, max_len=2)
        freq_itemsets_1_2['support'] = (freq_itemsets_1_2['support'] * self.df.shape[0]).astype(int)
        freq_itemsets_1_2['length'] = freq_itemsets_1_2['itemsets'].apply(len)

        freq_itemsets_2 = freq_itemsets_1_2[freq_itemsets_1_2['length'] == 2]
        freq_itemsets_2.drop('length', axis=1, inplace=True)

        freq_itemsets_1_2.drop('length', axis=1, inplace=True)
        self.freq_itemsets_1_2 = freq_itemsets_1_2

        return freq_itemsets_2

    #################################
    # STEP 2: Construct IAG (DONE)

    def step_2(self):
        freq_itemsets_2 = self.step_1()
        nodes = pd.DataFrame(freq_itemsets_2.pop('itemsets'))
        nodes[[0, 1]] = pd.DataFrame(nodes['itemsets'].tolist(), index=nodes.index)
        nodes.drop('itemsets', axis=1, inplace=True)
        nodes = nodes.values
        edge_weights = freq_itemsets_2.values
        unique_nodes = np.unique(nodes)

        adjncy = []
        xadj = [0]
        eweights = []
        for node in unique_nodes:
            where = np.array(np.where(nodes == node))
            where[1] = (where[1] + 1) % 2
            where = where.T
            for row, col in where:
                adjncy.append(nodes[row][col]) # maybe change index?
                eweights.append(edge_weights[row][0])
            xadj.append(xadj[-1] + where.shape[0])
        return adjncy, xadj, eweights

    #################################
    # STEP 3: Partition IAG using MLkP (DONE)

    def step_3(self):
        adjncy, xadj, eweights = self.step_2()
        cut_count, parts = pymetis.part_graph(self.num_parts, xadj=xadj, adjncy=adjncy, eweights=eweights)
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
        for i in range(len(dataset_modified)):
            for j in range(self.num_parts):
                intersect = parts_modified[j].intersection(dataset_modified[i])
                if len(intersect) > 2:
                    dataset_partitions[j].append(intersect)
        return dataset_partitions

    #################################
    # STEP 5: Mine freq itemsets on each partition using modified Apriori or FP-Growth (DONE)

    def step_5(self):
        dataset_partitions = self.step_4()
        trxn_ncoder = TransactionEncoder()
        result = []
        for i in dataset_partitions:
            
            #trxn_ncoder_arry = trxn_ncoder.fit(i).transform(i, sparse=True)
            trxn_ncoder_arry = trxn_ncoder.fit(i).transform(i)
            
            #df = pd.DataFrame.sparse.from_spmatrix(trxn_ncoder_arry, columns=trxn_ncoder.columns_)
            df = pd.DataFrame(trxn_ncoder_arry, columns=trxn_ncoder.columns_)

            freq_itemsets_trxn_part = fpgrowth(df, min_support=self.min_sup)
            freq_itemsets_trxn_part['length'] = freq_itemsets_trxn_part['itemsets'].apply(len)
            freq_itemsets_trxn_part = freq_itemsets_trxn_part[freq_itemsets_trxn_part['length'] > 2]
            freq_itemsets_trxn_part.drop('length', axis=1, inplace=True)
            freq_itemsets_trxn_part['support'] = (freq_itemsets_trxn_part['support'] * len(i)).astype(int)
            result.append(freq_itemsets_trxn_part)
        return result

    #################################
    # STEP 6: Find union of results from each partition (STEP 5) (DONE)

    def step_6(self):
        result = self.step_5()
        union = pd.DataFrame()
        for i in result:
            union = pd.concat([union, i], ignore_index=True)
        union = pd.concat([union, self.freq_itemsets_1_2], ignore_index=True)
        return union

    #################################
    # STEP 7: Generate association rules using Apriori-ap-genrules on freq itemsets (STEP 6) (DONE)
    def step_7(self):
        union = self.step_6()
        rules = association_rules(union, metric='confidence', min_threshold=self.min_conf)
        return rules