import pandas as pd
import numpy as np
from mlxtend.frequent_patterns import fpgrowth
from mlxtend.frequent_patterns import apriori
from mlxtend.preprocessing import TransactionEncoder
import time

file_name = 'testWithPaperData.csv'

df1 = pd.DataFrame([[1,'a',11],[2,'b',12],[3,'c',13]])
df2 = pd.DataFrame([[4,'d',21],[5,'e',22],[6,'f',23]])
df3 = pd.DataFrame([[7,'g',31],[8,'h',32],[9,'i',33]])

df_sep = pd.DataFrame([['*','*','*']])

list_ = [df1, df2, df3]
df = pd.DataFrame()

for i in list_:
    df = pd.concat([df, i, df_sep], ignore_index=True)

print(df)