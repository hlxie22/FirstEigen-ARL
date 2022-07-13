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
import pymetis
import time

#################################
# GLOBAL CONSTS

FILE_NAME = 'testWithPaperData.csv'
MIN_SUP = 0.1
MIN_CONF = 0.7
NUM_PARTS = 2 # k for MLkP
NUM_CUTS = 2 # METIS specific param

#################################
# SARL CLASS

class SARL:

    #################################
    # CONSTRUCTOR

    def __init__(self, file_name, min_sup, min_conf, num_parts, num_cuts):
        self.trxn_ncoder = TransactionEncoder()
        self.df = self.get_data(file_name)
        self.min_sup = min_sup
        self.min_conf = min_conf
        self.num_parts = num_parts
        self.num_cuts = num_cuts

    #################################
    # INTERNAL FUNCTIONS TO MAKE THE LATER STEPS MORE EFFICIENT



    #################################
    # GET DATA AND PRE-PROCESS

    def get_cat_and_num_dfs(self, file_name, threshold):
        df = pd.read_csv(file_name, dtype=str)
        df.fillna('', inplace=True)
        num = pd.DataFrame()
        for col in df:
            df_col = df[col]
            if pd.unique(df_col).shape[0] > threshold:
                num[col] = df.pop(col)
        cat = df
        return cat, num

    def num_to_cat(self, file_name, threshold):
        df = self.get_cat_and_num_dfs(file_name, threshold)[1]
        #col_mins = df.min()
        #col_maxs = df.max()
        #cols = list(df.columns)
        #for col in cols:
        #    df_col = df[col]
        #    if pd.unique(df_col).shape[0] > threshold:
        #        df[col + ' length'] = df_col.apply(lambda x: len(x))
        #        df[col + ' min'] = col_mins[col]
        #        df[col + ' max'] = col_maxs[col]
                # maybe delete original cols from df
                # remember to include threshold
        return df

    def get_data(self, file_name):
        #df = self.num_to_cat(file_name)
        df = pd.read_csv(file_name, dtype=str, header=None)
        df.fillna('', inplace=True)
        df = df.values.tolist()
        #df = self.trxn_ncoder.fit(df).transform(df, sparse=True)
        df = self.trxn_ncoder.fit(df).transform(df)
        #df = pd.DataFrame.sparse.from_spmatrix(df, columns=self.trxn_ncoder.columns_)
        df = pd.DataFrame(df, columns=self.trxn_ncoder.columns_)

        df.drop('', axis=1, inplace=True)

        return df

    #################################
    # STEP 1: Find freq 2-itemsets using Apriori (DONE)

    def step_1(self):
        freq_itemsets = apriori(self.df, min_support=self.min_sup, max_len=2)
        freq_itemsets['length'] = freq_itemsets['itemsets'].apply(lambda x: len(x))

        freq_itemsets_2 = freq_itemsets[freq_itemsets['length'] == 2]
        freq_itemsets_2.drop('length', axis=1, inplace=True)
        freq_itemsets_2['support'] = (freq_itemsets_2['support'] * self.df.shape[0]).astype(int)
        
        freq_itemsets['support'] = (freq_itemsets['support'] * self.df.shape[0]).astype(int)
        freq_itemsets.drop('length', axis=1, inplace=True)
        
        self.freq_itemsets = freq_itemsets


        return freq_itemsets_2

    #################################
    # STEP 2: Construct IAG (DONE)

    def step_2(self):
        freq_itemsets = self.step_1()
        nodes = pd.DataFrame(freq_itemsets.pop('itemsets'))
        nodes[[0, 1]] = pd.DataFrame(nodes['itemsets'].tolist(), index=nodes.index)
        nodes.drop('itemsets', axis=1, inplace=True)
        nodes = nodes.values
        freq_itemsets = freq_itemsets.values
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
                eweights.append(freq_itemsets[row][0])
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

        '''
        for i in dataset_modified:
            for j in range(self.num_parts):
                if i.issubset(parts_modified[j]):
                    dataset_partitions[j].append(list(i))
                    break
        '''
        return dataset_partitions

    #################################
    # STEP 5: Mine freq itemsets on each partition using modified Apriori or FP-Growth (DONE)

    def step_5(self):
        dataset_partitions = self.step_4()
        result = []
        for i in dataset_partitions:
            #trxn_ncoder_arry = self.trxn_ncoder.fit(i).transform(i, sparse=True)
            trxn_ncoder_arry = self.trxn_ncoder.fit(i).transform(i)
            #df = pd.DataFrame.sparse.from_spmatrix(trxn_ncoder_arry, columns=self.trxn_ncoder.columns_)
            df = pd.DataFrame(trxn_ncoder_arry, columns=self.trxn_ncoder.columns_)
            freq_itemsets_trxn_part = fpgrowth(df, min_support=self.min_sup)
            freq_itemsets_trxn_part['length'] = freq_itemsets_trxn_part['itemsets'].apply(lambda x: len(x))
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
        union = pd.concat([union, self.freq_itemsets], ignore_index=True)
        #union['length'] = union['itemsets'].apply(lambda x: len(x))
        #union = union[union['length'] > 2]
        #union.drop('length', axis=1, inplace=True)
        
        #union['support'] = (union['support']).astype(float) / self.df.shape[0]
        return union

    #################################
    # STEP 7: Generate association rules using Apriori-ap-genrules on freq itemsets (STEP 6) (DONE)
    def step_7(self):
        union = self.step_6()
        #rules = association_rules(union, metric='confidence', min_threshold=self.min_conf, support_only=True)
        rules = association_rules(union, metric='confidence', min_threshold=self.min_conf)
        return rules


test = SARL(FILE_NAME, MIN_SUP, MIN_CONF, NUM_PARTS, NUM_CUTS)
#start = time.time()
a = test.step_7()
#end = time.time()
#print(end-start)
print(a)

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