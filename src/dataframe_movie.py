from . import dataframe_visualizer
import matplotlib.pyplot as plt
import matplotlib.image as mpimg
from matplotlib.offsetbox import OffsetImage, AnnotationBbox
from matplotlib.animation import FuncAnimation, PillowWriter
import numpy as np
import os
import pandas as pd

class DataFrameMovie:
    """
    Class for creating a movie out of a dataframe with temporal splits.

    Attributes:
        dataframe (pd.DataFrame): The pandas DataFrame to animate.
        visualizer (DataFrameVisualizer): Instance of DataFrameVisualizer for generating plots.
    """

    def __init__(self, dataframe):
        self.dataframe = dataframe
        self.visualizer = dataframe_visualizer.DataFrameVisualizer(dataframe)

# =====================================
# Splits
# =====================================

    def create_m_splits(self, column, n):
        """
        Creates 'm' splits based on the specified column and number of time-based splits.
        """
        if column not in self.dataframe.columns:
            raise ValueError(f"Column {column} not found in the dataframe.")
        
        df_sorted = self.dataframe.sort_values(by=column).reset_index(drop=True)
        start_date = df_sorted[column].min()
        end_date = df_sorted[column].max()
        time_range = (end_date - start_date) / n

        split_ranges = [(start_date + i * time_range, start_date + (i + 1) * time_range) for i in range(n)]

        df_sorted['movie'] = None
        df_sorted['m_start'] = None
        df_sorted['m_end'] = None

        for i, (m_start, m_end) in enumerate(split_ranges):
            if i < n - 1:
                mask = (df_sorted[column] >= m_start) & (df_sorted[column] < m_end)
            else:
                # Ensure the last interval includes the final record
                mask = (df_sorted[column] >= m_start)
            
            df_sorted.loc[mask, 'movie'] = f'{i + 1}'
            df_sorted.loc[mask, 'm_start'] = m_start
            df_sorted.loc[mask, 'm_end'] = m_end

        # Special handling for the very last row if it still doesn't have a 'movie' value
        if df_sorted['movie'].isnull().any():
            df_sorted.loc[df_sorted.index[-1], 'movie'] = f'{n}'
            df_sorted.loc[df_sorted.index[-1], 'm_start'] = split_ranges[-1][0]
            df_sorted.loc[df_sorted.index[-1], 'm_end'] = split_ranges[-1][1]

        return df_sorted

    def create_m_splits_by_category(self, column):
        """
        Assigns a unique 'movie' value to each unique category in the specified column.
        
        The function will give each unique value of the column (e.g., countries) a different 'movie' label.
        """
        if column not in self.dataframe.columns:
            raise ValueError(f"Column {column} not found in the dataframe.")
        
        df_sorted = self.dataframe.copy()
        
        # Create a mapping from unique values to movie labels
        category_to_movie = {category: f'{i + 1}' for i, category in enumerate(df_sorted[column].unique())}
        
        # Create columns for 'movie', 'm_start', and 'm_end'
        df_sorted['movie'] = df_sorted[column].map(category_to_movie)

        return df_sorted

# =====================================
# Movies
# =====================================

    def animate_movie(self, plot_type, plot_params, duration_per_frame=1000, output_filename='movie', fig_length=10, fig_height=6, y_min=None, y_max=None, icon_paths=None, icon_columns=None, icon_zooms=None):
        """
        Animates the DataFrame with the 'movie' column using the specified plot type and parameters, with optional icons attached to lines.

        Parameters:
            plot_type (str): The type of plot to use (e.g., 'multiple_variable_lineplot').
            plot_params (dict): The parameters to pass to the plotting function.
            duration_per_frame (int): Time in milliseconds per frame (default: 1000).
            output_filename (str): The name of the output file (default: 'movie').
            fig_length (int): Length of final gif.
            fig_height (int): Height of final gif.
            y_min (float, optional): Minimum value for the y-axis. Computed dynamically if not provided.
            y_max (float, optional): Maximum value for the y-axis. Computed dynamically if not provided.
            icon_paths (list, optional): List of paths to icons to attach to lines.
            icon_columns (list, optional): List of columns where the icons should be attached.
            icon_zooms (list, optional): List of zoom levels for each icon.
        """
        if not os.path.exists('movies'):
            os.makedirs('movies')

        df_sorted = self.dataframe.sort_values(by='movie')

        # Get the range of the x and y axis based on the entire dataset (for fixing the axis)
        x_column = plot_params['x_column']
        y_columns = plot_params['y_columns']

        x_min, x_max = df_sorted[x_column].min(), df_sorted[x_column].max()

        # Load the icons and create a list of imageboxes with respective zoom levels
        imageboxes = []
        if icon_paths and icon_columns and icon_zooms:
            for path, zoom in zip(icon_paths, icon_zooms):
                img = mpimg.imread(path)
                imagebox = OffsetImage(img, zoom=zoom)
                imageboxes.append(imagebox)

        if y_min is None:
            y_min = df_sorted[y_columns].min().min()  # across all y columns
        if y_max is None:
            y_max = df_sorted[y_columns].max().max()  # across all y columns

        fig, ax = plt.subplots(figsize=(fig_length, fig_height))

        # Handle categorical x-axis values
        if df_sorted[x_column].dtype == 'object' or pd.api.types.is_categorical_dtype(df_sorted[x_column]):
            all_categories = sorted(df_sorted[x_column].unique())  # Get all unique categories
            df_sorted[x_column] = pd.Categorical(df_sorted[x_column], categories=all_categories, ordered=True)

        def update(frame):
            ax.clear()

            # Always accumulate data from previous splits
            current_data = df_sorted[df_sorted['movie'] <= frame]

            # Ensure x-axis is sorted before plotting
            current_data = current_data.sort_values(by=x_column)

            # Re-instantiate the visualizer with the filtered data
            self.visualizer = dataframe_visualizer.DataFrameVisualizer(current_data)

            if plot_type == 'movie_multiple_variable_lineplot':
                self.visualizer.movie_multiple_variable_lineplot(ax=ax, **plot_params)

            # Set static axis
            ax.set_xlim(x_min, x_max)
            ax.set_ylim(y_min, y_max)

            # Rotate x-axis labels and ensure all categories are shown
            plt.xticks(rotation=45)
            if df_sorted[x_column].dtype == 'category':
                # Set the x-axis ticks to include all categories, even if not all are present in the current frame
                ax.set_xticks(range(len(all_categories)))
                ax.set_xticklabels(all_categories)

            # Attach each icon to the respective column's last point
            if icon_paths and icon_columns and icon_zooms:
                for icon_column, imagebox in zip(icon_columns, imageboxes):
                    # Get the last point of the specified column to place the icon
                    if icon_column in current_data.columns:
                        x_pos = current_data[x_column].iloc[-1]
                        y_pos = current_data[icon_column].iloc[-1]

                        # Only proceed if x_pos and y_pos are valid
                        if pd.notna(x_pos) and pd.notna(y_pos):
                            ab = AnnotationBbox(imagebox, (x_pos, y_pos), frameon=False)
                            ax.add_artist(ab)

            # Set axis labels
            ax.set_xlabel(plot_params.get('x_column', ''))

        df_sorted['movie'] = df_sorted['movie'].astype(int)

        frames = sorted(df_sorted['movie'].unique())  # Order m splits ascending

        anim = FuncAnimation(fig, update, frames=frames, interval=duration_per_frame)

        output_path = os.path.join('movies', f'{output_filename}.gif')

        anim.save(output_path, writer=PillowWriter(fps=1000 // duration_per_frame))

        plt.close(fig)  # Close the figure to suppress any plot output in the notebook

        print(f"Animation saved as {output_path}")