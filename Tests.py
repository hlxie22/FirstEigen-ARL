#################################
# IMPORTS

from SARL import SARL
from DataProcessor import DataProcessor
from mlxtend.frequent_patterns import fpgrowth
from mlxtend.frequent_patterns import association_rules
import pandas as pd
import time

#################################
# FUNCTION FOR TESTING

# SARL FUNCTIONS

def test_general(file_name, threshold, min_sup, min_conf, num_parts):

    DP = DataProcessor(file_name)
    df = DP.get_SARL_data(threshold)

    start = time.time()

    sarl = SARL(df, min_sup, min_conf, num_parts)
    rules_1 = sarl.step_7()

    end = time.time()

    print('-' * 65, 'SARL RESULTS', '-' * 65)
    print(rules_1)
    print(f'time: {end - start}')
    
    #################################
    
    start = time.time()
    
    freq_itemsets = fpgrowth(df, min_support=min_sup)
    rules_2 = association_rules(freq_itemsets, metric='confidence', min_threshold=min_conf)

    end = time.time()

    print('-' * 65, 'FP-GROWTH RESULTS', '-' * 65)
    print(rules_2)
    print(f'time: {end - start}')

    print('-' * 65, 'INTERSECTION', '-' * 65)

    print(pd.merge(rules_1, rules_2, how='inner', on=['antecedents', 'consequents']))



def test_speed(file_name, threshold, min_sup, min_conf, num_parts, num_trials):

    DP = DataProcessor(file_name)
    df = DP.get_SARL_data(threshold)

    running_sum = 0

    for i in range(num_trials):

        start = time.time()

        sarl = SARL(df, min_sup, min_conf, num_parts)
        rules_1 = sarl.step_7()

        end = time.time()
        running_sum += end - start

    print('-' * 65, 'SARL RESULTS', '-' * 65)
    print(f'average time: {running_sum / num_trials}')
    
    #################################
    
    running_sum = 0

    for i in range(num_trials):

        start = time.time()
    
        freq_itemsets = fpgrowth(df, min_support=min_sup)
        rules_2 = association_rules(freq_itemsets, metric='confidence', min_threshold=min_conf)

        end = time.time()
        running_sum += end - start

    print('-' * 65, 'FP-GROWTH RESULTS', '-' * 65)
    print(f'average time: {running_sum / num_trials}')

# NumRelFinder FUNCTIONS

def test_NumRelFinder(file_name, corr_threshold, r_squared_threshold):

    DP = DataProcessor(file_name)
    df = DP.num

    relationships = NumRelFinder(df, corr_threshold, r_squared_threshold).store_relationships()

  
#################################
# RUNNING THE TESTS

# SARL:
  
test_general('anomaly.csv', 7, 0.1, 0.7, 2)
#test_general('CatTestDataSARL.csv', 7, 0.1, 0.7, 3)
#test_speed('CatTestDataSARL.csv', 7, 0.1, 0.7, 3, 1000)


# NumRelFinder: 

#test_NumRelFinder('CalCOFI Edited.csv', 0.7, 0.7)