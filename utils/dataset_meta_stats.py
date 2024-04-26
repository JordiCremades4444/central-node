"""Class with ONLY Statistical methods
"""

import pandas as pd
import numpy as np

class DatasetMetaStats:
    def __init__(self):
        try:
            pass  
        except Exception as e:
            print(f"An error occurred: {e}")

    def compare_colums(self, d1, d2, col1, col2, d1_name='d1', d2_name='d2'):
        """
        Description:
        ------------
        
        This method compares two columns of two dataframes and extracts
        the differences between them. It also gives some statistical info
        regarding their differences.
        
        
        Parameters:
        ------------
        
        d1,d2: pandas.DataFrames
            DataFrames with data to comparre
        d1_name,d2_name String
            Names of the DataFrames
        col1,col2: String
            Column names for d1 and d2
        
        Returns:
        ------------
        
        pandas.Dataframe d1_comp
            Rows that exist in d1 but not in d2 based on the value of 
            col1=col2
        
        pandas.Dataframe d2_comp
            Rows that exist in d2 but not in d1 based on the value of 
            col2=col1
        """
        try:
            items_in_both = d1[col1].isin(d2[col2])
            items_in_d1_only = ~items_in_both
            items_in_d2_only = ~d2[col2].isin(d1[col1])
            
            print(f"Number of items in {col1} in {d1_name}: {len(d1)}")
            print(f"Number of items in {col2} in {d2_name}: {len(d2)}")
            print("-" * 40)  # separator line
            print(f"Shared items in both {d1_name} and {d2_name}: {sum(items_in_both)}")
            print(f"Items in {col1} only in {d1_name}: {sum(items_in_d1_only)}")
            print(f"Items in {col2} only in {d2_name}: {sum(items_in_d2_only)}")
            print("-" * 40)  # separator line
            perc_common = sum(d1[col1].isin(d2[col2]))/len(d1)
            print(f"% in {d1_name} also in {d2_name}: {perc_common}")
            perc_common = sum(d2[col2].isin(d1[col1]))/len(d2)
            print(f"% in {d2_name} also in {d1_name}: {perc_common}")
            
            d1_comp = d1[items_in_d1_only].copy()
            d2_comp = d2[items_in_d2_only].copy()
            
            return d1_comp, d2_comp
        except Exception as e:
            print(f"An error occurred: {e}")
