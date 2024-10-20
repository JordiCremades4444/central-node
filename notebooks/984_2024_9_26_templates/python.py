# =====================================
# 0 - Configuration
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
# 0 - Plots
# =====================================

# Create an instance of DataFrameVisualizer
visualizer = dataframe_visualizer.DataFrameVisualizer(df)

# Define the plot configurations for each subplot
plot_configs = [
    {
        'plot_type': 'lineplot',
        'x_column': 'XXX',
        'y_columns': ['XXX', 'XXX'],
        'colors': ['blue', 'orange'],
        'styles': [':','-']
    },
    {
        'plot_type': 'scatterplot',
        'x_column': 'XXX',
        'y_columns': ['XXX', 'XXX'],
        'colors': ['blue', 'orange'],
    },
]

figure_params = {
    'n_plots': 2,
    'fig_length': 12,
    'fig_height': 6,
    'x_min': 0,
    'x_max': 0,
    'y_min': 0,
    'y_max': 0,
    'x_rotation': 45,
    'share_x': True,
    'share_y': False
}


# Call the plot method to handle the entire flow
visualizer.plot(
    method='static', 
    figure_params=figure_params,
    plot_configs=plot_configs
)

# =====================================
# Pivot table
# =====================================

START_DATE = "'YYYY-MM-DD'"
END_DATE = "'YYYY-MM-DD'"

df['p_creation_date'] = pd.to_datetime(df['p_creation_date'])

cond1 = df['p_creation_date'] >= pd.to_datetime(START_DATE)
cond2 = df['p_creation_date'] <= pd.to_datetime(END_DATE)

df_pivoted = df[cond1 & cond2].pivot(index='XXX', columns='XXX', values=['XXX'])

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

START_DATE = "'YYYY-MM-DD'"
END_DATE = "'YYYY-MM-DD'"

params = [
    {'name':'start_date', 'value': str(START_DATE)},
    {'name':'end_date', 'value': str(END_DATE)}
]

q.prepare_query(
    'XXX.sql'
    ,params=params
    ,to_load_file=None
    ,load_from_to_load_file=None
    
)

df  = q.query_run_starburst()

