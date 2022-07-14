#################################
# IMPORTS

import pandas as pd
from mlxtend.preprocessing import TransactionEncoder

#################################
# GLOBAL CONSTS

FILE_NAME = 'TestDataDP.csv'
#FILE_NAME = 'TestDataSARL.csv'

#################################
# DATA PROCESSOR CLASS

class DataProcessor:

    #################################
    # CONSTRUCTOR

    def __init__(self, file_name):
        df = pd.read_csv(file_name, header=None, parse_dates=True, infer_datetime_format=True)

        self.cat = df.select_dtypes(include='object')
        self.date = df.select_dtypes(include='datetime')
        self.num = df.select_dtypes(include='number')

    #################################
    # GET DATA AND PROCESS

    def num_to_cat(self, threshold):
        self.num = self.num.astype(str)
        #self.num.fillna('', inplace=True)
        for col in self.num:
            if pd.unique(self.num[col]).shape[0] > threshold:
                self.num[f'{col} length'] = self.num[col].apply(len)
                # add whatever other metadata cols here
                #self.num.drop[col] # maybe drop the cols with metadata?

    def get_SARL_data(self):
        trxn_ncoder = TransactionEncoder()
        df = pd.concat([self.cat, self.num], axis=1)
        print(df)
        #df = pd.concat([self.cat, self.num.set_index(self.cat.index)], axis=1)

        df = df.astype(str)
        df.columns = df.columns.astype(str)
        df = df.values.tolist()

        #df = trxn_ncoder.fit(df).transform(df, sparse=True)
        df = trxn_ncoder.fit(df).transform(df)

        #df = pd.DataFrame.sparse.from_spmatrix(df, columns=trxn_ncoder.columns_)
        df = pd.DataFrame(df, columns=trxn_ncoder.columns_)

        return df

a = DataProcessor(FILE_NAME)
'''
print('-'*20, 'CAT', '-'*20)
print(a.cat)
print('-'*20, 'NUM', '-'*20)
print(a.num)
print('-'*20, 'DATE', '-'*20)
print(a.date)

print('-'*20, 'NUM_TO_CAT', '-'*20)
a.num_to_cat(4)
print(a.num)
'''

a.num_to_cat(4)
print('-'*20, 'GET_SARL_DATA', '-'*20)
b = a.get_SARL_data()
print()
print(b)