# =====================================
# Configuration
# =====================================

import os
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
    {
        'plot_type': 'histogram',
        'y_columns': ['XXX', 'XXX'],
        'colors': ['blue', 'orange'], # default None
        'legend': True, # default True
        'bins': 10 # default 10
    },
        {
        'plot_type': 'histogram_accumulated',
        'y_columns': ['XXX', 'XXX'],
        'colors': ['blue', 'orange'], # default None
        'legend': True, # default True
        'bins': 10 # default 10
    },
    {
        # default LB = Q1 - 1.5*IQR, UB = Q3 + 1.5*IQR
        # IQR = Q3 - Q1, Q1 = 25th percentile, Q3 = 75th percentile
        'plot_type': 'box_and_whiskers', 
        'y_columns': ['XXX', 'XXX'],
        'colors': ['blue', 'orange'], # default None
        'legend': True, # default True
    },
]

figure_params = {
    'n_plots': 1, # default 1
    'fig_length': 12, # default 10
    'fig_height': 6, # default 6
    'x_rotation': 45, # default 45
    'share_x': True, # default False
    'share_y': False, # default False
    'x_limits': [(0,100),(0,200)], # default None
    'y_limits': [(0,100),(0,200)], # default None
    'log_axis': ['x','y','both'], # default None
    'title': 'XXX' # default None
}

# Call the plot method to handle the entire flow
v.plot(
    figure_params=figure_params,
    plot_params=plot_params
)

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

