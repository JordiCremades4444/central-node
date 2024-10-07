import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation, PillowWriter
import numpy as np

class DataFrameVisualizer:
    """
    Class to visualize dataframes using various types of plots, including animations.

    Attributes:
        dataframe (pd.DataFrame): The pandas DataFrame to visualize.
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
    
    def plot(self, figure_params=None, plot_params=None):
        """
        Handles the entire flow of figure creation and plotting.
        
        Parameters:
            figure_params (dict, optional): Dictionary of parameters for create_figure method, including:
                                            - n_plots (int): Number of subplots (axes) to create (default: 1).
                                            - fig_length (int): The length of the figure (default: 10).
                                            - fig_height (int): The height of the figure (default: 6).
                                            - x_rotation (int): Rotation angle for x-axis labels (default: 45).
                                            - x_limit (tuple): Tuple of (x_min, x_max) for x-axis limits.
                                            - y_limit (tuple): Tuple of (y_min, y_max) for y-axis limits.
            plot_params (list of dict, optional): A list of dictionaries, where each dictionary contains
                                                parameters for each subplot, including:
                                                - plot_type (str): Type of plot ('lineplot' or 'scatterplot').
                                                - x_column (str): The x-axis column.
                                                - y_columns (list of str): List of y-axis columns.
                                                - colors (list of str, optional): List of colors for the plot.
                                                - styles (list of str, optional): List of line/marker styles.
        """
        
        # Default figure parameters if none provided
        if figure_params is None:
            figure_params = {
                'n_plots': 1,
                'fig_length': 10,
                'fig_height': 6,
                'x_rotation': 45,
                'x_limits': None,
                'y_limits': None,
            }

        # Create the figure and axes
        fig, axes = self.create_figure(**figure_params)
        
        # Ensure axes is always a list even for a single subplot
        if figure_params['n_plots'] == 1:
            axes = [axes]

        # Loop through each plot configuration
        for i, config in enumerate(plot_params):
            ax = axes[i]
            plot_type = config.get('plot_type')
            x_column = config.get('x_column')
            y_columns = config.get('y_columns')
            colors = config.get('colors', None)
            styles = config.get('styles', None)
            legend = config.get('legend')

            # Call static plotting method
            if plot_type == 'lineplot':
                self.multiple_variable_lineplot(x_column=x_column, y_columns=y_columns, ax=ax, colors=colors, styles=styles, legend=legend)
            elif plot_type == 'scatterplot':
                self.multiple_variable_scatterplot(x_column=x_column, y_columns=y_columns, ax=ax, colors=colors, legend=legend)
            elif plot_type == 'barplot':
                self.multiple_variable_barplot(x_column=x_column, y_columns=y_columns, ax=ax, colors=colors, legend=legend)

        plt.show()

    def create_figure(self, n_plots=1, fig_length=10, fig_height=6, x_rotation=45, share_x=False, share_y=False, x_limits=None, y_limits=None, title=None):
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
            x_limits (tuple, optional): Tuple of (x_min, x_max) to set the x-axis limits. If None, axis is set automatically.
            y_limits (tuple, optional): Tuple of (y_min, y_max) to set the y-axis limits. If None, axis is set automatically.
            title (str, optional): Title of the figure.
        """
        self.fig_size = (fig_length, fig_height)

        if n_plots == 1:
            fig, ax = plt.subplots(figsize=self.fig_size)
            ax.tick_params(axis='x', rotation=x_rotation)

            # Set axis limits together for x and y
            if x_limits is not None and len(x_limits) == 2:
                ax.set_xlim(left=x_limits[0], right=x_limits[1])
            if y_limits is not None and len(y_limits) == 2:
                ax.set_ylim(bottom=y_limits[0], top=y_limits[1])

            if title:
                fig.text(0.5, 0.95, title, ha='center', fontsize=16)  # Place title at the top of the figure
            
            return fig, ax

        else:
            fig, axs = plt.subplots(n_plots, figsize=self.fig_size, sharex=share_x, sharey=share_y)
            if not isinstance(axs, (list, np.ndarray)):
                axs = [axs]  # Ensure axs is always a list
            for ax in axs:
                ax.tick_params(axis='x', rotation=x_rotation)

                # Set axis limits together for x and y
                if x_limits is not None and len(x_limits) == 2:
                    ax.set_xlim(left=x_limits[0], right=x_limits[1])
                if y_limits is not None and len(y_limits) == 2:
                    ax.set_ylim(bottom=y_limits[0], top=y_limits[1])

            if title:
                fig.text(0.5, 0.95, title, ha='center', fontsize=16)  # Place title at the top of the figure
            
            return fig, axs
   

    # =====================================
    # Plots methods
    # =====================================
    def multiple_variable_lineplot(self, x_column, y_columns, ax, colors=None, styles=None, legend=True):
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
        # Use the passed dataframe, or default to self.dataframe if not provided
        self._validate_columns([x_column] + y_columns)
        colors = self._get_colors(colors, len(y_columns))
        styles = self._get_styles(styles, len(y_columns))

        for y_column, color, style in zip(y_columns, colors, styles):
            ax.plot(self.dataframe[x_column], self.dataframe[y_column], color=color, linestyle=style, label=y_column)

        if legend:
            ax.legend(loc='best')

    def multiple_variable_scatterplot(self, x_column, y_columns, ax, colors=None, legend=True):
        """
        Creates a scatter plot for multiple y variables against an x variable.

        Parameters:
            x_column (str): Column name for x-axis.
            y_columns (list of str): List of columns for y-axis.
            ax (matplotlib.axes.Axes): Axis object to plot on.
            colors (list, optional): List of colors for each y-column.
        """
        # Use the passed dataframe, or default to self.dataframe if not provided
        self._validate_columns([x_column] + y_columns)
        colors = self._get_colors(colors, len(y_columns))

        for y_column, color in zip(y_columns, colors):
            ax.scatter(self.dataframe[x_column], self.dataframe[y_column], color=color, label=y_column)

        if legend:
            ax.legend(loc='best')

    def multiple_variable_barplot(self, x_column, y_columns, ax, colors=None, legend=True, bar_width=0.3):
        """
        Creates a bar plot for multiple y variables against an x variable.

        Parameters:
            x_column (str): Column name for x-axis.
            y_columns (list of str): List of columns for y-axis.
            ax (matplotlib.axes.Axes): Axis object to plot on.
            colors (list, optional): List of colors for each y-column.
            legend (bool, optional): Whether to show a legend. Defaults to True.
            bar_width (float, optional): Width of the bars. Defaults to 0.3.
        """
        # Validate that columns exist in the dataframe
        self._validate_columns([x_column] + y_columns)
        colors = self._get_colors(colors, len(y_columns))

        x_positions = np.arange(len(self.dataframe[x_column]))  # Bar positions

        for i, (y_column, color) in enumerate(zip(y_columns, colors)):
            ax.bar(x_positions + i * bar_width, self.dataframe[y_column], color=color, width=bar_width, label=y_column)

        # Set x-ticks to be in the center of the grouped bars
        ax.set_xticks(x_positions + bar_width * (len(y_columns) - 1) / 2)
        ax.set_xticklabels(self.dataframe[x_column])

        if legend:
            ax.legend(loc='best')