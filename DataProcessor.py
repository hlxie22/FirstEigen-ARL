#################################
# IMPORTS

import pandas as pd
from mlxtend.preprocessing import TransactionEncoder

#################################
# DATA PROCESSOR CLASS

class DataProcessor:

    #################################
    # CONSTRUCTOR

    def __init__(self, file_name, first_n_rows, cols):
        df = pd.read_csv(file_name, parse_dates=True, infer_datetime_format=True, nrows=first_n_rows, usecols=cols)
        df = df.convert_dtypes()

        self.cat = df.select_dtypes(include='string')
        self.date = df.select_dtypes(include='datetime')
        self.num = df.select_dtypes(include='number')

    #################################
    # GET DATA AND PROCESS

    def num_to_cat(self, threshold):
        self.num = self.num.astype(str)
        for col in self.num:
            if pd.unique(self.num[col]).shape[0] > threshold:
                len_col = self.num[col].apply(lambda x: f'{col}_length: {len(x)}')
                self.num[f'{col} length'] = len_col
                # add whatever other metadata cols here
                #self.num.drop[col] # maybe drop the cols with metadata?

    def get_SARL_data(self, threshold):
        self.num_to_cat(threshold)
        trxn_ncoder = TransactionEncoder()
        df = pd.concat([self.cat, self.num], axis=1)
        #df = pd.concat([self.cat, self.num.set_index(self.cat.index)], axis=1)

        df = df.astype(str)
        df.columns = df.columns.astype(str)
        df = df.values.tolist()

        #df = trxn_ncoder.fit(df).transform(df, sparse=True)
        df = trxn_ncoder.fit(df).transform(df)

        #df = pd.DataFrame.sparse.from_spmatrix(df, columns=trxn_ncoder.columns_)
        df = pd.DataFrame(df, columns=trxn_ncoder.columns_)

        return df, trxn_ncoder.columns_