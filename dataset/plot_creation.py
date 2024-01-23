import argparse
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

def create_plot(input):

    df = pd.read_csv(input)

    pivot_df = df.pivot(index='Distribution', columns='Algorithm', values='Duration')

    # Plot the bar chart
    ax = pivot_df.plot(kind='bar', figsize=(10, 6), rot=0)

    # Add labels and title
    plt.xlabel('Distribution')
    plt.ylabel('Duration')
    plt.title('Bar Plot for Different Distributions and Algorithms')
    plt.legend(title='Algorithm')

    for p in ax.patches:
        if p.get_height() != 0:
            ax.annotate(str(p.get_height()), (p.get_x() + p.get_width() / 2., p.get_height()),
                        ha='center', va='center', xytext=(0, 10), textcoords='offset points')


    # Show the plot
    plt.show()

if __name__ == "__main__":
  parser = argparse.ArgumentParser(description="Generate data of certain distribution, size and dimension and save it to a CSV file.")
  parser.add_argument("-i", "--input", help="Total number of points", required=True)

  args = parser.parse_args()
  create_plot(args.input)