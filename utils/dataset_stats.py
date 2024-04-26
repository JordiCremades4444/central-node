"""Class with ONLY Statistical methods
"""

import pandas as pd
import numpy as np

class DatasetStats:
    def __init__(self):
        try:
            pass  
        except Exception as e:
            print(f"An error occurred: {e}")

    def calculate_weights(self, df, col1, f_weights=None, f_weights_cumsum=None, f_threshold=None):
            """
            Description:
            ------------
            
            This method calculates the weights of each row for a given column
            and adds it as an extra column. Then it also sorts the value based
            on this value, computes the cumsum and adds a label 1/0 if they are
            in the first 0.8 of the cumulative distribution.
            
            
            Parameters:
            ------------
            
            df: pandas.DataFrames
                DataFrames with data
            col1: String
                Name of the column used to track weight
            f_weights, f_weights_cumsum, f_threshold: Boolean
                To exclude or not the column. If True it extists
            
            Returns:
            ------------
            
            pandas.Dataframe d1_comp
                Dataframe with the weight columns added
            """
            
            try:
                df = df.copy()
                total = df[col1].sum()
                weights = '(w)_'+str(col1)
                # add weights columns
                df[weights] = df[col1].apply(lambda x: x/total)
                df = df.sort_values(by=weights, ascending=False)
                weights_cumsum = '(w_cumsum)_'+str(col1)
                # add cumulative column
                df[weights_cumsum] = df[weights].cumsum()
                # add flag for the 0.8 contributor group
                flag = '(flag_0.8_group)_'+str(col1)
                # Or its the first value that completes the 0.8 threshold or is below 0.8
                df[flag] = np.where((df[weights_cumsum] < 0.8) | (df.index == (df[weights_cumsum] >= 0.8).idxmax()),1,0)
                if not f_threshold:
                    df.drop(flag, axis=1, inplace=True)
                if not f_weights:
                    df.drop(weights, axis=1, inplace=True)
                if not f_weights_cumsum:
                    df.drop(weights_cumsum, axis=1, inplace=True)
                return df
            except Exception as e:
                print(f"An error occurred: {e}")