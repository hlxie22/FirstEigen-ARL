#################################
# IMPORTS

import pandas as pd
import numpy as np
from sklearn.linear_model import Lasso

#################################
# NumRelFinder CLASS

class NumRelFinder:

    #################################
    # CONSTRUCTOR

    def __init__(self, df, corr_threshold, r_squared_threshold):
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
        X_train = self.df[predictor_variables]
        y_train = self.df[target_variable]
        lasso.fit(X_train, y_train)
        score = lasso.score(X_train, y_train)
        if score > self.r_squared_threshold:
          model_weights.append(lasso.coef_)
          model_intercepts.append(lasso.intercept_)
          model_scores.append(score)
          target_variables.append(target_variable)
          num_models += 1

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
          term = "{weight} * ({column_name}) + ".format(weight = model_weights[i][j],   column_name = self.correlations[target_variable][j])
          relationship += term
        relationship += str(model_intercepts[i]) + " = " + str(target_variable)
        score = model_scores[i]
        output = "Numerical Relationship #{index}: {relationship}; R^2 Score: {score}".format(index = i, relationship = relationship, score = score)
        print(output)
        relationships.append(output)

      return relationships