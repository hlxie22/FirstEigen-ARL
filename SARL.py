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
# GLOBAL CONSTS

FILE_NAME = 'testWithPaperData.csv'
THRESHOLD = 20

# CATEGORICAL
MIN_SUP = 0.1
MIN_CONF = 0.7
NUM_PARTS = 2 # k for MLkP

# NUMERICAL
CORR_THRESHOLD = 0.7
R_SQUARED_THRESHOLD = 0.7

#################################
# SARL CLASS

class SARL:

    #################################
    # CONSTRUCTOR

    def __init__(self, file_name, threshold, min_sup, min_conf, num_parts, corr_threshold, r_squared_threshold):
        self.trxn_ncoder = TransactionEncoder()
        cat, self.df_num = get_cat_and_num_dfs(file_name, threshold)
        self.df = self.get_data(file_name)
        
        # CATEGORICAL
        self.min_sup = min_sup
        self.min_conf = min_conf
        self.num_parts = num_parts

        # NUMERICAL
        self.corr_threshold = corr_threshold
        self.r_squared_threshold = r_squared_threshold 

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

    def get_data(self, file_name):
        df = pd.read_csv(file_name, dtype=str, header=None)
        df.fillna('', inplace=True)
        df = df.values.tolist()
        
        df = self.trxn_ncoder.fit(df).transform(df, sparse=True)
        #df = self.trxn_ncoder.fit(df).transform(df)
        
        df = pd.DataFrame.sparse.from_spmatrix(df, columns=self.trxn_ncoder.columns_)
        #df = pd.DataFrame(df, columns=self.trxn_ncoder.columns_)

        df.drop('', axis=1, inplace=True)

        return df

    #################################
    # STEP 1: Find freq 2-itemsets using Apriori (TO-DO: OPTIMIZE)

    def step_1(self):
        freq_itemsets = apriori(self.df, min_support=self.min_sup, max_len=2)
        freq_itemsets['support'] = (freq_itemsets['support'] * self.df.shape[0]).astype(int)
        freq_itemsets['length'] = freq_itemsets['itemsets'].apply(lambda x: len(x))

        freq_itemsets_2 = freq_itemsets[freq_itemsets['length'] == 2]
        freq_itemsets_2.drop('length', axis=1, inplace=True)

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
        return union

    #################################
    # STEP 7: Generate association rules using Apriori-ap-genrules on freq itemsets (STEP 6) (DONE)
    def step_7(self):
        union = self.step_6()
        rules = association_rules(union, metric='confidence', min_threshold=self.min_conf)
        return rules

    #################################
    # NRF STEP 1 (BUILD ADJACENCY MATRIX OF CORRELATED VARIABLES):

    def nrf_step1(self):
      df_corr = self.df_num.corr()
      adjacency_matrix = {}
      for col in df_corr.columns:
        adj = []
        for i in range(df_corr[col].size):
          corr = df_corr[col][i]
          if abs(corr) > self.corr_threshold and corr != 1:
            adj.append(df_corr.columns[i])
        if len(adj) > 0:
          adjacency_matrix[col] = adj
      return adjacency_matrix

    #################################
    # NRF STEP 2 (PARTITION ADJACENCY MATRIX INTO DISJOINT CONNECTED COMPONENTS):

    # "connected_components" is a list of lists. Each list within "connected_components" contains the items in each connected component of the graph
  
    def nrf_step2(self):
      adjacency_matrix = self.nrf_step1()
      num_components = 0
      connected_components = []
      groupings = {}

      for key in adjacency_matrix:
        groupings[key] = 0

      for key in adjacency_matrix:
        if groupings[key] == 0:
          num_components += 1
          component = []
          queue = [key]
          while len(queue) > 0:
            next_item = queue.pop(0)
            for item in adjacency_matrix[next_item]:
              if groupings[item] == 0:
                queue.append(item)
            groupings[next_item] = num_components
            component.append(next_item)
          connected_components.append(component)
    
    return connected_components
      
    #################################
    # NRF STEP 3 (TRAIN LINEAR REGRESSION ON EACH CONNECTED COMPONENT):

    def nrf_step3(self):
      connected_components = self.nrf_step2()
      num_models = len(connected_components)
      model_weights = []
      model_intercepts = []
      model_scores = []

      zeroes = np.array([0])
      zeroes = np.repeat(zeroes, df_num.shape[0])

      lasso = Lasso()

      for component in connected_components:
        X_train = df_num[component]
        y_train = zeroes
        lasso.fit(X_train, y_train)
        score = lasso.score(X_train, y_train)
        if score > self.r_squared_threshold:
          model_weights.append(lasso.coef_)
          model_intercepts.append(lasso.intercept_)
          model_scores.append(score)

      return num_models, model_weights, model_intercepts, model_scores
      
    #################################
    # NRF STEP 4 (STORE FINAL RELATIONSHIPS):

    def nrf_step4(self):

      num_models, model_weights, model_intercepts, model_scores = self.nrf_step3()
      relationships = []

      for i in range(num_models):
        relationship = ""
        for j in range(len(model_weights[i])):
          term = "{weight} * {column_name} + ".format(weight = model_weights[i][j],   column_name = df_num[connected_components[i]].columns[j])
          relationship += term
        relationship += str(model_intercepts[i]) + " = 0"
        score = model_weights[i]
        output = "Numerical Relationship #{index}: {relationship}; R^2 Score: {score}".format(index = i, relationship = relationship, score = score)
        print(output)
        relationships.append(output)

      return relationships


#################################
# TEST AREA

test = SARL(FILE_NAME, MIN_SUP, MIN_CONF, NUM_PARTS)
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