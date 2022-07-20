#################################
# IMPORTS

from SARL import SARL
from DataProcessor import DataProcessor
from mlxtend.frequent_patterns import fpgrowth
from mlxtend.frequent_patterns import association_rules
import pandas as pd
import time

#################################
# FUNCITON FOR TESTING

def test_general(file_name, threshold, min_sup, min_conf, num_parts):

    DP = DataProcessor(file_name)
    df, col_names = DP.get_SARL_data(threshold)

    start = time.time()

    sarl = SARL(df, min_sup, min_conf, num_parts)
    rules_1 = sarl.step_7()

    end = time.time()

    rules_1[['antecedents', 'consequents']] = rules_1[['antecedents', 'consequents']].applymap(lambda x: tuple(col_names[i] for i in tuple(x)))

    print('-' * 65, 'SARL RESULTS', '-' * 65)
    print(rules_1)
    print(f'time: {end - start}')
    
    #################################
    
    start = time.time()
    
    freq_itemsets = fpgrowth(df, min_support=min_sup)
    rules_2 = association_rules(freq_itemsets, metric='confidence', min_threshold=min_conf)

    end = time.time()

    rules_2[['antecedents', 'consequents']] = rules_2[['antecedents', 'consequents']].applymap(lambda x: tuple(col_names[i] for i in tuple(x)))

    print('-' * 65, 'FP-GROWTH RESULTS', '-' * 65)
    print(rules_2)
    print(f'time: {end - start}')

    print('-' * 65, 'INTERSECTION', '-' * 65)

    print(pd.merge(rules_1, rules_2, how='inner', on=['antecedents', 'consequents']))
    print(col_names)



def test_speed(file_name, threshold, min_sup, min_conf, num_parts, num_trials):

    DP = DataProcessor(file_name)
    df, col_names = DP.get_SARL_data(threshold)

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

#################################
# RUNNING THE TESTS

#test_general('anomaly2.csv', 10, 0.1, 0.7, 2)

test_general('CatTestDataSARL.csv', 10, 0.1, 0.7, 2)
#test_speed('CatTestDataSARL.csv', 7, 0.1, 0.7, 3, 100)

#DP = DataProcessor('anomaly2.csv')