import matplotlib.pyplot as plt
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
            "blue": "#1f77b4", 
            "orange": "#ff7f0e", 
            "green": "#2ca02c", 
            "red": "#d62728",
            "purple": "#9467bd", 
            "brown": "#8c564b", 
            "pink": "#e377c2", 
            "gray": "#7f7f7f",
            "olive": "#bcbd22", 
            "cyan": "#17becf"
        }
        self.styles = {
            "-": "-", 
            "--": "--", 
            ":": ":"
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

    def _get_bar_width(self, bar_width):
        """
        Validates and retrieves the bar width for plotting.

        Parameters:
            bar_width (float or None): The width of the bars. If None, defaults to 0.8.

        Returns:
            float: The bar width to use.
        """
        if bar_width is None:
            return 0.8
        if not isinstance(bar_width, (int, float)):
            raise ValueError("bar_width must be a number (int or float).")
        if bar_width <= 0:
            raise ValueError("bar_width must be greater than 0.")
        return bar_width

    def _get_bins(self, bins):
        """
        Validates and retrieves the number of bins for plotting.

        Parameters:
            bins (int or None): The number of bins. If None, defaults to 10.

        Returns:
            int: The number of bins to use.
        """
        if bins is None:
            return 10
        if not isinstance(bins, (int)):
            raise ValueError("bins must be an int.")
        if bins <= 0:
            raise ValueError("bins must be greater than 0.")
        return bins

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
                                            - x_limits (list of tuple, optional): List of tuples of (x_min, x_max) to set the x-axis limits for each subplot. If None, axis is set automatically.
                                            - y_limits (list of tuple, optional): List of tuples of (y_min, y_max) to set the y-axis limits for each subplot. If None, axis is set automatically.
                                            - log_axis (list of str, optional): List of axes to set to logarithmic scale ('x', 'y', or 'both') for each subplot. If None, no log scale is applied.
                                            - title (str, optional): Title of the figure.

            plot_params (list of dict): A list of dictionaries, where each dictionary contains
                                                parameters for each subplot, including:
                                                - plot_type (str): Type of plot.
                                                - x_column (str): The x-axis column.
                                                - y_columns (list of str): List of y-axis columns.
                                                - colors (list of str, optional): List of colors for the plot.
                                                - styles (list of str, optional): List of line/marker styles.
                                                - legend (bool, optional): Whether to show a legend.
                                                - bar_width (float, optional): Width of the bars for barplot.
                                                - bins (int, optional): Number of bins for histogram.
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
                'log_axis': None,
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
            bar_width = config.get('bar_width', None)
            bins = config.get('bins', None)

            # Call static plotting method
            if plot_type == 'lineplot':
                self.multiple_variable_lineplot(x_column=x_column, y_columns=y_columns, ax=ax, colors=colors, styles=styles, legend=legend)
            elif plot_type == 'scatterplot':
                self.multiple_variable_scatterplot(x_column=x_column, y_columns=y_columns, ax=ax, colors=colors, legend=legend)
            elif plot_type == 'barplot':
                self.multiple_variable_barplot(x_column=x_column, y_columns=y_columns, ax=ax, colors=colors, legend=legend, bar_width=bar_width)
            elif plot_type == 'histogram':
                self.multiple_variable_histogram(y_columns=y_columns, ax=ax, bins=bins, colors=colors, legend=legend)
            elif plot_type == 'boxplot_and_whiskers':
                self.multiple_variable_box_and_whisker_plot(y_columns=y_columns, ax=ax, colors=colors, legend=legend)

        plt.show()

    def create_figure(self, n_plots=1, fig_length=10, fig_height=6, x_rotation=45, share_x=False, share_y=False, x_limits=None, y_limits=None, title=None, log_axis=None):
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
            x_limits (list of tuple, optional): List of tuples of (x_min, x_max) to set the x-axis limits for each subplot. If None, axis is set automatically.
            y_limits (list of tuple, optional): List of tuples of (y_min, y_max) to set the y-axis limits for each subplot. If None, axis is set automatically.
            title (str, optional): Title of the figure.
            log_axis (list of str, optional): List of axes to set to logarithmic scale ('x', 'y', or 'both') for each subplot. If None, no log scale is applied.
        """
        self.fig_size = (fig_length, fig_height)

        if n_plots == 1:
            fig, ax = plt.subplots(figsize=self.fig_size)
            ax.tick_params(axis='x', rotation=x_rotation)

            # Set axis limits for x and y
            if x_limits is not None and len(x_limits) == 1:
                ax.set_xlim(left=x_limits[0][0], right=x_limits[0][1])
            if y_limits is not None and len(y_limits) == 1:
                ax.set_ylim(bottom=y_limits[0][0], top=y_limits[0][1])

            # Set logarithmic scale if specified
            if log_axis is not None and len(log_axis) == 1:
                if log_axis[0] == 'x':
                    ax.set_xscale('log')
                elif log_axis[0] == 'y':
                    ax.set_yscale('log')
                elif log_axis[0] == 'both':
                    ax.set_xscale('log')
                    ax.set_yscale('log')

            if title:
                fig.text(0.5, 0.95, title, ha='center', fontsize=16)  # Place title at the top of the figure
            
            return fig, ax

        else:
            fig, axs = plt.subplots(n_plots, figsize=self.fig_size, sharex=share_x, sharey=share_y)
            if not isinstance(axs, (list, np.ndarray)):
                axs = [axs]  # Ensure axs is always a list
            for i, ax in enumerate(axs):
                ax.tick_params(axis='x', rotation=x_rotation)

                # Set axis limits for x and y
                if x_limits is not None and len(x_limits) > i:
                    ax.set_xlim(left=x_limits[i][0], right=x_limits[i][1])
                if y_limits is not None and len(y_limits) > i:
                    ax.set_ylim(bottom=y_limits[i][0], top=y_limits[i][1])

                # Set logarithmic scale if specified
                if log_axis is not None and len(log_axis) > i:
                    if log_axis[i] == 'x':
                        ax.set_xscale('log')
                    elif log_axis[i] == 'y':
                        ax.set_yscale('log')
                    elif log_axis[i] == 'both':
                        ax.set_xscale('log')
                        ax.set_yscale('log')

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

    def multiple_variable_barplot(self, x_column, y_columns, ax, colors=None, legend=True, bar_width=None):
        """
        Creates a bar plot for multiple y variables against an x variable.

        Parameters:
            x_column (str): Column name for x-axis.
            y_columns (list of str): List of columns for y-axis.
            ax (matplotlib.axes.Axes): Axis object to plot on.
            colors (list, optional): List of colors for each y-column.
            legend (bool, optional): Whether to show a legend. Defaults to True.
            bar_width (float, optional): Width of the bars. Defaults to 0.8.
        """
        # Validate that columns exist in the dataframe
        self._validate_columns([x_column] + y_columns)
        colors = self._get_colors(colors, len(y_columns))
        bar_width = self._get_bar_width(bar_width)

        n_bars = len(y_columns)
        x_positions = np.arange(len(self.dataframe[x_column]))  # Group positions
        individual_bar_width = bar_width / n_bars  # Width of each bar

        for i, (y_column, color) in enumerate(zip(y_columns, colors)):
            # Position each bar within its group
            bar_positions = x_positions + (i - (n_bars - 1) / 2) * individual_bar_width
            ax.bar(
                bar_positions,
                self.dataframe[y_column],
                color=color,
                width=individual_bar_width,
                label=y_column,
            )

        # Set x-ticks to the center of each group of bars
        ax.set_xticks(x_positions)
        ax.set_xticklabels(self.dataframe[x_column], ha='center')

        # Optionally add a legend
        if legend:
            ax.legend(loc='best')

    def multiple_variable_histogram(self, y_columns, ax, bins=10, colors=None, legend=True):
        """
        Creates normalized histograms for the specified columns.

        Parameters:
            y_columns (list of str): List of column names for y-axis.
            ax (matplotlib.axes.Axes): Axis object to plot on.
            bins (int, optional): Number of bins to use in the histogram.
            colors (list of str, optional): List of colors for the histogram bars.
            legend (bool, optional): Whether to show a legend.
        """
        self._validate_columns(y_columns)
        colors = self._get_colors(colors, len(y_columns))
        bins = self._get_bins(bins)

        for y_column, color in zip(y_columns, colors):
            # Drop null values before plotting
            self.dataframe[y_column].dropna().plot.hist(bins=bins, alpha=0.5, color=color, ax=ax, density=True, label=y_column, edgecolor='black')

        if legend:
            ax.legend(loc='best')

    def multiple_variable_box_and_whisker_plot(self, y_columns, ax, colors=None, legend=True):
        """
        Creates a box and whisker plot for multiple columns.

        Parameters:
            y_columns (list of str): List of column names for y-axis.
            ax (matplotlib.axes.Axes): Axis object to plot on.
            colors (list of str, optional): List of colors for the box plots.
            legend (bool, optional): Whether to show a legend.
        """
        self._validate_columns(y_columns)
        colors = self._get_colors(colors, len(y_columns))

        for y_column, color in zip(y_columns, colors):
            # Drop null values before plotting
            self.dataframe[y_column].dropna().plot.box(
                boxprops=dict(color=color),
                whiskerprops=dict(color=color),
                capprops=dict(color=color),
                medianprops=dict(color=color),
                flierprops=dict(markeredgecolor=color),
                ax=ax,
                label=y_column,
                vert=False,
                widths=0.9
            )

        if legend:
            ax.legend(y_columns, loc='best')