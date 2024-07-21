"""Class to visualize dataframes
"""

import pandas as pd
import matplotlib.pyplot as plt


class DataFrameVisualizer:
    def __init__(self, dataframe):
        """
        Initializes the DataFrameVisualizer with a pandas DataFrame.

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
            "cyan": "#17becf",
        }

    def one_variable_lineplot(self, x_column, y_column, color=None):
        """
        Creates a line plot for the specified columns.

        Parameters:
        x_column (str): The name of the column to be used for the x-axis.
        y_column (str): The name of the column to be used for the y-axis.
        """
        # Check if the columns exist in the dataframe
        if (
            x_column not in self.dataframe.columns
            or y_column not in self.dataframe.columns
        ):
            raise ValueError(
                f"Columns {x_column} and/or {y_column} not found in the dataframe."
            )

        # Check if the color is valid
        if color not in self.colors:
            raise ValueError(
                f"Color {color} not found. Available colors: {list(self.colors.keys())}"
            )

        # Plotting the line plot
        plt.figure(figsize=(10, 6))
        plt.plot(
            self.dataframe[x_column],
            self.dataframe[y_column],
            linestyle="-",
            color=self.colors[color],
        )
        plt.xlabel(x_column)
        plt.ylabel(y_column)
        plt.title(f"Line Plot of {y_column} vs {x_column}")
        plt.show()

    def multiple_variable_lineplot(self, x_column, y_columns, colors=None):
        """
        Creates a line plot for multiple y columns.

        Parameters:
        x_column (str): The name of the column to be used for the x-axis.
        y_columns (list of str): The names of the columns to be used for the y-axis.
        colors (list of str): The colors for the lines. Default is None, which cycles through available colors.
        """
        # Check if the columns exist in the dataframe
        if x_column not in self.dataframe.columns:
            raise ValueError(f"Column {x_column} not found in the dataframe.")
        for y_column in y_columns:
            if y_column not in self.dataframe.columns:
                raise ValueError(f"Column {y_column} not found in the dataframe.")

        # Check if the colors list is provided
        if colors is None:
            colors = list(self.colors.values())[: len(y_columns)]
        else:
            if len(colors) != len(y_columns):
                raise ValueError(
                    "The number of colors provided does not match the number of y_columns."
                )
            for color in colors:
                if color not in self.colors:
                    raise ValueError(
                        f"Color {color} not found. Available colors: {list(self.colors.keys())}"
                    )
            colors = [self.colors[color] for color in colors]

        # Plotting the line plot
        plt.figure(figsize=(10, 6))
        for y_column, color in zip(y_columns, colors):
            plt.plot(
                self.dataframe[x_column],
                self.dataframe[y_column],
                linestyle="-",
                color=color,
                label=y_column,
            )
        plt.xlabel(x_column)
        plt.ylabel("Values")
        plt.title(f"Line Plot of {y_columns} vs {x_column}")
        plt.legend()
        plt.show()
