import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation, PillowWriter
import numpy as np

class DataFrameVisualizer:
    """
    Class to visualize dataframes using various types of plots, including animations.

    Attributes:
        dataframe (pd.DataFrame): The pandas DataFrame to visualize.
        colors (dict): A dictionary of color names and their corresponding hex codes.
        fig_size (tuple): The default figure size (width, height) for plots.
    """

    def __init__(self, dataframe):
        """
        Initializes the DataFrameVisualizer with a pandas DataFrame and optional figure size.

        Parameters:
            dataframe (pd.DataFrame): The pandas DataFrame to visualize.
        """
        self.dataframe = dataframe
        self.colors = {
            "blue": "#1f77b4", "orange": "#ff7f0e", "green": "#2ca02c", "red": "#d62728",
            "purple": "#9467bd", "brown": "#8c564b", "pink": "#e377c2", "gray": "#7f7f7f",
            "olive": "#bcbd22", "cyan": "#17becf"
        }
        self.styles = {
            "-": "-", "--": "--", ":": ":"
        }

    def _validate_columns(self, columns):
        """
        Validates that the specified columns exist in the dataframe.

        Parameters:
            columns (list of str): A list of column names to check.

        Raises:
            ValueError: If any of the columns are not found in the dataframe.
        """
        missing_cols = [col for col in columns if col not in self.dataframe.columns]
        if missing_cols:
            raise ValueError(f"Columns {missing_cols} not found in the dataframe.")

    def _get_colors(self, colors, n):
        """
        Validates and retrieves colors for plotting.

        Parameters:
            colors (list of str or None): A list of color names or None. If None, default colors are used.
            n (int): The number of colors required.

        Returns:
            list of str: List of color hex codes.
        """
        if colors is None:
            return list(self.colors.values())[:n]
        if len(colors) != n:
            raise ValueError("The number of colors provided does not match the number of columns.")
        return [self.colors.get(color, self.colors['blue']) for color in colors]

    def _get_styles(self, styles, n):
        """
        Validates and retrieves line styles for plotting.

        Parameters:
            styles (list of str or None): A list of style names or None. If None, default solid lines are used.
            n (int): The number of styles required.

        Returns:
            list of str: List of style strings.
        """
        if styles is None:
            return [self.styles["-"]] * n
        if len(styles) != n:
            raise ValueError("The number of styles provided does not match the number of columns.")
        return [self.styles.get(style, self.styles['-']) for style in styles]
    
    # =====================================
    # Plot
    # =====================================  
    def plot(self, method='static', figure_params=None, plot_configs=None):
        """
        Handles the entire flow of figure creation and plotting based on the method.

        Parameters:
            method (str): Type of plotting ('static'). Default is 'static'.
            figure_params (dict, optional): Dictionary of parameters for create_figure method, including:
                                            - n_plots (int): Number of subplots (axes) to create (default: 1).
                                            - fig_length (int): The length of the figure (default: 10).
                                            - fig_height (int): The height of the figure (default: 6).
                                            - x_rotation (int): Rotation angle for x-axis labels (default: 45).
                                            - share_x (bool): If True, subplots share the x-axis (default: False).
                                            - share_y (bool): If True, subplots share the y-axis (default: False).
            plot_configs (list of dict, optional): A list of dictionaries, where each dictionary contains
                                                parameters for each subplot, including:
                                                - plot_type (str): 'lineplot' or 'scatterplot'
                                                - x_column (str): The x-axis column
                                                - y_columns (list of str): List of y-axis columns
                                                - ax (matplotlib.axes.Axes): The axis object to plot on.
                                                - colors (list of str, optional): List of colors for the plot.
                                                - styles (list of str, optional): List of line/marker styles.
        """
        if method == 'static':
            # Default figure parameters if none provided
            if figure_params is None:
                figure_params = {
                    'n_plots': 1,
                    'fig_length': 10,
                    'fig_height': 6,
                    'x_rotation': 45,
                    'share_x': False,
                    'share_y': False
                }

            # Create the figure and axes
            fig, axes = self.create_figure(**figure_params)
            
            # Ensure axes is always a list even for a single subplot
            if figure_params['n_plots'] == 1:
                axes = [axes]
            
            # Loop through each plot configuration
            for i, plot_config in enumerate(plot_configs):
                ax = axes[i]
                plot_type = plot_config.get('plot_type')
                x_column = plot_config.get('x_column')
                y_columns = plot_config.get('y_columns')
                colors = plot_config.get('colors', None)
                styles = plot_config.get('styles', None)
                
                # Call the appropriate plot method
                if plot_type == 'lineplot':
                    self.multiple_variable_lineplot(x_column=x_column, y_columns=y_columns, ax=ax, colors=colors, styles=styles)
                elif plot_type == 'scatterplot':
                    self.multiple_variable_scatterplot(x_column=x_column, y_columns=y_columns, ax=ax, colors=colors)
            
            # Display the plot
            plt.show()

    # =====================================
    # Figure
    # =====================================
    def create_figure(self, n_plots=1, fig_length=10, fig_height=6, x_rotation=45, share_x=False, share_y=False, x_min=None, x_max=None, y_min=None, y_max=None):
        """
        Defines the figure and axes based on the number of plots.
        Sets the axes properties such as labels and x-axis rotation.
        Returns the figure and axes.

        Parameters:
            n_plots (int): Number of plots (axes) to create (default: 1).
            fig_length (int): The length of the figure (default: 10).
            fig_height (int): The height of the figure (default: 6).
            x_rotation (int, optional): Rotation angle for x-axis labels (default: 45).
            share_x (bool, optional): If True, subplots share the x-axis (default: False).
            share_y (bool, optional): If True, subplots share the y-axis (default: False).
            x_min (float, optional): Minimum x-axis value.
            x_max (float, optional): Maximum x-axis value.
            y_min (float, optional): Minimum y-axis value.
            y_max (float, optional): Maximum y-axis value.
        """
        self.fig_size = (fig_length, fig_height)

        if n_plots == 1:
            fig, ax = plt.subplots(figsize=self.fig_size)
            ax.tick_params(axis='x', rotation=x_rotation)

            # Set axis limits independently
            if x_min is not None:
                ax.set_xlim(left=x_min)
            if x_max is not None:
                ax.set_xlim(right=x_max)
            if y_min is not None:
                ax.set_ylim(bottom=y_min)
            if y_max is not None:
                ax.set_ylim(top=y_max)

            return fig, ax

        else:
            fig, axs = plt.subplots(n_plots, figsize=self.fig_size, sharex=share_x, sharey=share_y)
            if not isinstance(axs, (list, np.ndarray)):
                axs = [axs]  # Ensure axs is always a list
            for ax in axs:
                ax.tick_params(axis='x', rotation=x_rotation)

                # Set axis limits independently
                if x_min is not None:
                    ax.set_xlim(left=x_min)
                if x_max is not None:
                    ax.set_xlim(right=x_max)
                if y_min is not None:
                    ax.set_ylim(bottom=y_min)
                if y_max is not None:
                    ax.set_ylim(top=y_max)

            return fig, axs

    # =====================================
    # Plots methods
    # =====================================
    def multiple_variable_lineplot(self, x_column, y_columns, ax, colors=None, styles=None):
        """
        Creates a line plot for multiple y variables against an x variable.
        If the x_column is a pd.datetime, then the x-axis will not be too crowded.

        Parameters:
            x_column (str): Column name for x-axis.
            y_columns (list of str): List of columns for y-axis.
            ax (matplotlib.axes.Axes): Axis object to plot on.
            colors (list, optional): List of colors for each y-column.
            styles (list, optional): List of line styles for each y-column.
        """
        self._validate_columns([x_column] + y_columns)
        colors = self._get_colors(colors, len(y_columns))
        styles = self._get_styles(styles, len(y_columns))

        for y_column, color, style in zip(y_columns, colors, styles):
            ax.plot(self.dataframe[x_column], self.dataframe[y_column], color=color, linestyle=style)

    def multiple_variable_scatterplot(self, x_column, y_columns, ax, colors=None):
        """
        Creates a scatter plot for multiple y variables against an x variable.
        If the x_column is a pd.datetime, then the x-axis will not be too crowded.

        Parameters:
            x_column (str): Column name for x-axis.
            y_columns (list of str): List of columns for y-axis.
            ax (matplotlib.axes.Axes): Axis object to plot on.
            colors (list, optional): List of colors for each y-column.
        """
        self._validate_columns([x_column] + y_columns)
        colors = self._get_colors(colors, len(y_columns))

        for y_column, color in zip(y_columns, colors):
            ax.scatter(self.dataframe[x_column], self.dataframe[y_column], color=color)