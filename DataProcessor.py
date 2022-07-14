#################################
# IMPORTS

import numpy as np
import pandas as pd
from mlxtend.preprocessing import TransactionEncoder

#################################
# GLOBAL CONSTS

FILE_NAME = 'testWithPaperData.csv'

#################################
# DATA PROCESSOR CLASS

class DataProcessor:

    #################################
    # CONSTRUCTOR

    def __init__(self, file_name):
        self.file_name = file_name

    #################################
    # GET DATA AND PROCESS

    def sep_cat_num_date(self): # (DONE)
        df = pd.read_csv(self.file_name, header=None, parse_dates=True, infer_datetime_format=True)
        
        cat = df.select_dtypes(include='object')
        num = df.select_dtypes(include='number')
        date = df.select_dtypes(include='datetime')
        
        return cat, num, date

    def num_to_cat(self):
        cat, num, date = self.sep_cat_num_date()
        num.fillna('', inplace=True)
        for col in df:
            df_col = df[col]
            if pd.unique(df_col).shape[0] > threshold:
                num[col] = df.pop(col)
        cat = df
        return cat, num

    def get_data(self, file_name):


        df.fillna('', inplace=True)
        df = df.values.tolist()
        
        df = self.trxn_ncoder.fit(df).transform(df, sparse=True)
        #df = self.trxn_ncoder.fit(df).transform(df)

        df = pd.DataFrame.sparse.from_spmatrix(df, columns=self.trxn_ncoder.columns_)
        #df = pd.DataFrame(df, columns=self.trxn_ncoder.columns_)

        return df