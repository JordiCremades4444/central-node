import pandas as pd
import matplotlib.pyplot as plt


class DataFrameVisualizer:
    """
    Class to visualize dataframes using various types of plots.

    Attributes:
        dataframe (pd.DataFrame): The pandas DataFrame to visualize.
        colors (dict): A dictionary of color names and their corresponding hex codes.
        fig_size (tuple): The default figure size (width, height) for plots.
    """

    def __init__(self, dataframe, fig_size=(10, 6)):
        """
        Initializes the DataFrameVisualizer with a pandas DataFrame and optional figure size.

        Parameters:
            dataframe (pd.DataFrame): The pandas DataFrame to visualize.
            fig_size (tuple, optional): The default figure size (width, height) for plots. Default is (10, 6).
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
            "cyan": "#17becf",
        }
        self.styles = {
            "s1": "-",   # Solid Line
            "s2": "--",  # Dashed Line
            "s3": "-.",  # Dash-Dot Line
            "s4": ":",   # Dotted Line
            "s5": "- -", # Loose Dashes
            "s6": "....",# Tightly Dotted Line
            "s7": "-. -.",# Loose Dash-Dot
            "s8": "- -", # Double-Dash Line
            "s9": "-. .",# Dot-Dash Line
            "s10": "- - -",# Triple-Dash Line
        }
        self.fig_size = fig_size  # Default figure size

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

        Raises:
            ValueError: If the number of colors does not match the number of columns or if any color is invalid.
        """
        if colors is None:
            # Use default colors if none are provided
            return list(self.colors.values())[:n]
        if len(colors) != n:
            raise ValueError(
                "The number of colors provided does not match the number of columns."
            )
        color_list = []
        for color in colors:
            if color is None:
                # Handle None color explicitly if it is passed
                color_list.append(
                    list(self.colors.values())[0]
                )  # Use the first available color
            elif color not in self.colors:
                raise ValueError(
                    f"Color {color} not found. Available colors: {list(self.colors.keys())}"
                )
            else:
                color_list.append(self.colors[color])
        return color_list

    def _get_styles(self, styles, n):
        """
        Validates and retrieves line styles for plotting.

        Parameters:
            styles (list of str or None): A list of style names or None. If None, default styles are used.
            n (int): The number of styles required.

        Returns:
            list of str: List of style strings.

        Raises:
            ValueError: If the number of styles does not match the number of columns or if any style is invalid.
        """
        if styles is None:
            # Use default solid lines if none are provided
            return [self.styles["s1"]] * n
        if len(styles) != n:
            raise ValueError(
                "The number of styles provided does not match the number of columns."
            )
        style_list = []
        for style in styles:
            if style not in self.styles:
                raise ValueError(
                    f"Style {style} not found. Available styles: {list(self.styles.keys())}"
                )
            style_list.append(self.styles[style])
        return style_list
    
    def one_variable_lineplot(self, x_column, y_column, color=None):
        """
        Creates a line plot for a single y variable against an x variable.
        If the x_column is a pd.datetime, then the x axis will not be too crowded

        Parameters:
            x_column (str): The name of the column to be used for the x-axis.
            y_column (str): The name of the column to be used for the y-axis.
            color (str, optional): The color of the line. Must be one of the predefined colors.

        Raises:
            ValueError: If the specified columns are not found in the dataframe or if the color is invalid.
        """
        self._validate_columns([x_column, y_column])
        # Handle default color if not provided
        color = self._get_colors([color], 1)[0]
        plt.figure(figsize=self.fig_size)
        plt.plot(
            self.dataframe[x_column],
            self.dataframe[y_column],
            linestyle="-",
            color=color,
        )
        plt.xlabel(x_column)
        plt.ylabel(y_column)
        plt.xticks(rotation=45)
        plt.show()

    def multiple_variable_lineplot(self, x_column, y_columns, colors=None, style=None):
        """
        Creates a line plot for multiple y variables against an x variable.
        If the x_column is a pd.datetime, then the x axis will not be too crowded

        Parameters:
            x_column (str): The name of the column to be used for the x-axis.
            y_columns (list of str): The names of the columns to be used for the y-axis.
            colors (list of str, optional): The colors for the lines. Default is None, which cycles through available colors.
            style (list of str, optional): The line styles for the plots. Default is solid lines.

        Raises:
            ValueError: If the specified x column or y columns are not found in the dataframe or if the colors are invalid.
        """
        self._validate_columns([x_column] + y_columns)
        colors = self._get_colors(colors, len(y_columns))
        styles = self._get_styles(style, len(y_columns))
        plt.figure(figsize=self.fig_size)
        for y_column, color, linestyle in zip(y_columns, colors, styles):
            plt.plot(
                self.dataframe[x_column],
                self.dataframe[y_column],
                linestyle=linestyle,
                color=color,
                label=y_column,
            )
        plt.xlabel(x_column)
        plt.ylabel("Values")
        plt.xticks(rotation=45)
        plt.legend()
        plt.show()
