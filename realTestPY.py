import pandas as pd
import numpy as np
from mlxtend.frequent_patterns import fpgrowth
from mlxtend.frequent_patterns import apriori
from mlxtend.preprocessing import TransactionEncoder
import time

union = pd.DataFrame()
print(union)
print()

clients1 = {'clientFirstName': ['Jon','Maria','Bruce','Lili'],
            'clientLastName': ['Smith','Lam','Jones','Chang'],
            'country': ['US','Canada','Italy','China']
           }
df1 = pd.DataFrame(clients1, columns= ['clientFirstName', 'clientLastName','country'])



clients2 = {'clientFirstName': ['Bill','Jack','Elizabeth','Jenny'],
            'clientLastName': ['Jackson','Green','Gross','Sing'],
            'country': ['UK','Germany','Brazil','Japan']
           }
df2 = pd.DataFrame(clients2, columns= ['clientFirstName', 'clientLastName','country'])



union = pd.concat([union, df1], ignore_index=True)
print (union)
print()

union = pd.concat([union, df2], ignore_index=True)
print (union)
print()
print(union == pd.concat([df1, df2], ignore_index=True))