import pandas as pd
import numpy as np
from mlxtend.frequent_patterns import fpgrowth
from mlxtend.frequent_patterns import apriori
from mlxtend.preprocessing import TransactionEncoder
import time

df = pd.read_csv('anomaly.csv')
df.drop(['CutomerID', 'Customer_Phone','LoanAmount','InterestEarned'], axis=1, inplace=True)
df.dropna(inplace=True)

start = time.time()
fpgrowth(df, min_support=0.6)
end = time.time()
print(end - start)