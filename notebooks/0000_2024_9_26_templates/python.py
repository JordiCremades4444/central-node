# =====================================
# Configuration
# =====================================

import matplotlib.pyplot as plt
import os
import pandas as pd
import seaborn as sns
import sys

# Move two levels up (to the project root) and append the `src` folder
src_path = os.path.abspath(os.path.join(os.getcwd(), '..', '..'))

# Append src to sys.path
sys.path.append(src_path)

from src import query_engines, dataframe_visualizer

q = query_engines.QueryEngines()

# =====================================
# Plots
# =====================================

# Create an instance of DataFrameVisualizer
v = dataframe_visualizer.DataFrameVisualizer(df)

# Define the plot configurations for each subplot
plot_params = [
    {
        'plot_type': 'lineplot', 
        'x_column': 'XXX', 
        'y_columns': ['XXX', 'XXX'],
        'colors': ['blue', 'orange'], # default None
        'styles': [':','-'], # default None
        'legend': True # default True
    },
    {
        'plot_type': 'scatterplot',
        'x_column': 'XXX',
        'y_columns': ['XXX', 'XXX'],
        'colors': ['blue', 'orange'], # default None
        'legend': True # default True
    },
    {
        'plot_type': 'barplot',
        'x_column': 'XXX',
        'y_columns': ['XXX', 'XXX'],
        'colors': ['blue', 'orange'], # default None
        'legend': True, # default True
        'bar_width': 0.8 # default 0.8
    },
]

figure_params = {
    'n_plots': 1, # default 1
    'fig_length': 12, # default 10
    'fig_height': 6, # default 6
    'x_rotation': 45, # default 45
    'share_x': True, # default False
    'share_y': False, # default False
    'x_limits': None, # default None
    'y_limits': None, # default None
    'title': 'XXX' # default None
}

# Call the plot method to handle the entire flow
v.plot(
    figure_params=figure_params,
    plot_params=plot_params
)

# =====================================
# Pivot table
# =====================================

START_DATE = "'YYYY-MM-DD'"
END_DATE = "'YYYY-MM-DD'"

df['p_creation_date'] = pd.to_datetime(df['p_creation_date'])

cond1 = df['p_creation_date'] >= pd.to_datetime(START_DATE)
cond2 = df['p_creation_date'] <= pd.to_datetime(END_DATE)

df_pivoted = df[cond1 & cond2].pivot_table(index=['XXX'], columns=['XXX'], values=['XXX'], aggfunc=['sum'])

# Flatten the multiindex columns 
df_pivoted.columns = [f'{col[0]}__{col[1]}' for col in df_pivoted.columns]
df_pivoted = df_pivoted.reset_index()

df_pivoted = df_pivoted.fillna(0)

# =====================================
# Prints 
# =====================================
pd.set_option('display.max_rows', None) # to print all rows

pd.reset_option('display.max_rows') # reset previous configuration

# =====================================
# Query engines
# =====================================

QUERY_NAME = 'XXX' # With sql
START_DATE = "'YYYY-MM-DD'"
END_DATE = "'YYYY-MM-DD'"

params = [
    {'name':'start_date', 'value': str(START_DATE)},
    {'name':'end_date', 'value': str(END_DATE)}
]

q.prepare_query(
    QUERY_NAME
    ,params=params
    ,to_load_file=QUERY_NAME
    ,load_from_to_load_file=None
    
)

df  = q.query_run_starburst()

