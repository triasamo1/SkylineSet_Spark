# -*- coding: utf-8 -*-
"""point_dataset_creation.ipynb

Automatically generated by Colaboratory.

"""

import sys
import csv
import argparse
import numpy as np
import os
from sklearn.preprocessing import MinMaxScaler

def generate_data(data_distribution, total_points, dimensions, output_name):
  if data_distribution == "uniform":
    data = np.random.uniform(0, 1, (total_points, dimensions))
  elif data_distribution == "normal":
    data = np.random.normal(0, 1, (total_points, dimensions))
    minmax = MinMaxScaler()
    data = minmax.fit_transform(np.array(data))
  elif data_distribution == "correlated":
    data = np.array([np.linspace(0.15, 0.85, total_points) + np.random.normal(scale=0.05, size=total_points) for _ in range(dimensions)]).T
  elif data_distribution == "anticorrelated":
    data = np.array([1 -np.linspace(0.2, 0.8, total_points) + np.random.normal(scale=0.05, size=total_points) if i == 0 else np.linspace(0.2, 0.8, total_points) + np.random.normal(scale=0.05, size=total_points) for i in range(dimensions)]).T

  folder_path = '../input/dimensions' + str(dimensions) +"/"
  os.makedirs(folder_path, exist_ok=True)
  with open(folder_path + output_name, 'w', newline='') as file:
    np.savetxt(folder_path + output_name, data, delimiter=' ')

if __name__ == "__main__":
  parser = argparse.ArgumentParser(description="Generate data of certain distribution, size and dimension and save it to a CSV file.")
#   parser.add_argument("-dist", "--distribution", choices=["uniform", "normal", "correlated", "anticorrelated"], help="Data distribution type", required=True)
  parser.add_argument("-p", "--points", type=int, help="Total number of points", required=True)
  parser.add_argument("-dim", "--dimensions", type=int, help="Number of dimensions", required=True)
#   parser.add_argument("-o", "--output", help="Output CSV file name", required=True)

  distributions = ['uniform', 'normal', 'correlated', 'anticorrelated']

  args = parser.parse_args()

  for distribution in distributions:
    generate_data(distribution, args.points, args.dimensions, distribution + '' + str(args.points) + '.txt')


