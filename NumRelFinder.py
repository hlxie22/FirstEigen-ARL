# This will search for linear relationships between numerical values. 

import pandas as pd
import numpy as np
from sklearn.linear_model import Lasso

#################################
# GLOBAL CONSTS

FILE_NAME = 'TestDataNRF.csv'
CORR_THRESHOLD = 0.7
MODEL_SCORE_THRESHOLD = 0.7

df = pd.read_csv(FILE_NAME)

df_num = df.select_dtypes(include=[np.number])
df_corr = df_num.corr()

# Create the adjacency matrix

adjacency_matrix = {}

for col in df_corr.columns:
  adj = []
  #for i in range(len(df_corr[col].size)):
  for i in range(df_corr[col].size):
    corr = df_corr[col][i]
    if abs(corr) > CORR_THRESHOLD and corr != 1:
      adj.append(df_corr.columns[i])
  if len(adj) > 0:
    adjacency_matrix[col] = adj

# Partition the adjacency matrix into disjoint connected components
# "connected_components" is a list of lists. Each list within "connected_components" contains the items in each connected component of the graph
# Based off the algorithm presented here: https://stackoverflow.com/questions/8124626/finding-connected-components-of-adjacency-matrix-graph

num_components = 0
connected_components = []
groupings = {}

for key in adjacency_matrix:
  groupings[key] = 0

for key in adjacency_matrix:
  if groupings[key] == 0:
    num_components += 1
    component = []
    queue = [key]
    while len(queue) > 0:
      next_item = queue.pop(0)
      for item in adjacency_matrix[next_item]:
        if groupings[item] == 0:
          queue.append(item)
      groupings[next_item] = num_components
      component.append(next_item)
    connected_components.append(component)

# Train linear regression on each connected component (use L1 regularization)
# Each value in model_scores is the R^2 value
# Throw out relationships that are relatively weak, based on R^2 value


model_weights = []
model_intercepts = []
model_scores = []

zeroes = np.array([0])
zeroes = np.repeat(zeroes, df.shape[0])

lasso = Lasso()

for component in connected_components:
  X_train = df[component]
  y_train = zeroes
  lasso.fit(X_train, y_train)
  if lasso.score() > MODEL_SCORE_THRESHOLD:
    model_weights.append(lasso.coef_)
    model_intercepts.append(lasso.intercept_)
    model_scores.append(lasso.score())

num_models = len(connected_components)

    
# TODO: Round the coefficients, because most relationships will likely be simple ones involving integer coefficients. Some other operation might work better as well

# Print output relationships and store final relationships

relationships = []

for i in range(num_models):
  relationship = ""
  for j in range(len(model_weights[i])):
    term = "{weight} * {column_name} + ".format(weight = model_weights[i][j], column_name = df[connected_components[i]].columns[j])
    relationship += term
  relationship += model_intercepts[i][0] + " = 0"
  output = "Numerical Relationship #{index}: {relationship}".format(index = i, relationship = relationship)
  print(output)
  relationships.append(output)
