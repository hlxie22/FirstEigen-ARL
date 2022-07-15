#################################
# IMPORTS

import SARL
import DataProcessor
import time

#################################
# GLOBAL CONSTS

FILE_NAME = 'CatTestDataSARL.csv'
MIN_SUP = 0.1
MIN_CONF = 0.7
NUM_PARTS = 2 # k for MLkP
THRESHOLD = 7

#################################
# MAIN CODE

DP = DataProcessor.DataProcessor(FILE_NAME)
df = DP.get_SARL_data(THRESHOLD)

sarl = SARL.SARL(df, MIN_SUP, MIN_CONF, NUM_PARTS)
rules = sarl.step_7()
print(rules)