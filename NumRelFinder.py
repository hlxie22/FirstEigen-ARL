#################################
# IMPORTS

import pandas as pd
import numpy as np
from sklearn.linear_model import Lasso
from sklearn.preprocessing import StandardScaler 
from sklearn.decomposition import PCA
from sklearn.model_selection import train_test_split
import matplotlib.pyplot as plt

#################################
# NumRelFinder CLASS

class NumRelFinder:

    #################################
    # CONSTRUCTOR

    def __init__(self, df, corr_threshold, r_squared_threshold, round = True):
        self.df = df
        self.corr_threshold = corr_threshold
        self.r_squared_threshold = r_squared_threshold
  
    #################################
    # STEP 1: FIND_CORRELATION_GROUPS (BUILD DICTIONARY OF CORRELATED VARIABLES):

    def find_correlation_groups(self):
      df_corr = self.df.corr()
      correlations = {}
      for col in df_corr.columns:
        adj = []
        for i in range(df_corr[col].size):
          corr = df_corr[col][i]
          if abs(corr) > self.corr_threshold and corr != 1:
            adj.append(df_corr.columns[i])
        if len(adj) > 0:
          correlations[col] = adj
      self.correlations = correlations
      return correlations
      
    #################################
    # STEP 2: TRAIN_MODELS (TRAIN LINEAR REGRESSION ON EACH CORRELATION GROUP):

    def train_models(self):
      correlations = self.find_correlation_groups()
      num_models = 0
      model_weights = []
      model_intercepts = []
      model_scores = []
      target_variables = []

      lasso = Lasso()

      for target_variable, predictor_variables in correlations.items():
        X = self.df[predictor_variables]
        y = self.df[target_variable]
        lasso.fit(X, y)
        score = lasso.score(X, y)
        print("Original Score: {}".format(score))
        if score > self.r_squared_threshold:
          lasso.coef_ = np.around(lasso.coef_, decimals = 1)
          lasso.intercept_= np.around(lasso.intercept_, decimals = 1)
          score = lasso.score(X, y)
          print("New Score: {}".format(score))
          if score > self.r_squared_threshold:
            model_weights.append(lasso.coef_)
            model_intercepts.append(lasso.intercept_)
            model_scores.append(score)
            target_variables.append(target_variable)
            num_models += 1
        self.model_weights = model_weights
        self.model_intercepts = model_intercepts
        self.target_variables = target_variables
      print("Number of relationships generated: {}".format(num_models))
      return num_models, target_variables, model_weights, model_intercepts, model_scores
      
    #################################
    # STEP 3: STORE_RELATIONSHIPS (STORE FINAL RELATIONSHIPS):

    def store_relationships(self):

      num_models, target_variables, model_weights, model_intercepts, model_scores = self.train_models()
      relationships = []

      for i in range(num_models):
        relationship = ""
        target_variable = target_variables[i]
        for j in range(len(model_weights[i])):
          weight = model_weights[i][j]
          column_name = self.correlations[target_variable][j]
          term = ""
          if weight != 0:
            term = "{weight} * ({column_name}) + ".format(weight = weight, column_name = column_name)
          relationship += term
        relationship += str(model_intercepts[i]) + " = " + str(target_variable)
        score = model_scores[i]
        output = "Numerical Relationship #{index}: {relationship}; R^2 Score: {score}".format(index = i, relationship = relationship, score = score)
        print(output)
        relationships.append(output)

      return relationships