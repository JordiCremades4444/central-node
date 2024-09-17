import pandas as pd
from scipy import stats
import sys

# Ensure this path is correctly pointing to your repository root
sys.path.append("c:/Users/Jordi Cremades/Documents/Repos/central-node")

from src.dataframe_visualizer import DataFrameVisualizer


class DataFrameStatistics:
    """
    Class for performing statistical analysis on a dataframe.

    Attributes:
        dataframe (pd.DataFrame): The pandas DataFrame to analyze.
        alpha (float): Significance level for statistical tests. Default is 0.05.
    """

    def __init__(self, dataframe, alpha=0.05):
        """
        Initializes the DataFrameStatistics with a pandas DataFrame and optional alpha for significance tests.

        Parameters:
            dataframe (pd.DataFrame): The pandas DataFrame containing the data.
            alpha (float, optional): The significance level for the tests. Default is 0.05.
        """
        self.dataframe = dataframe
        self.alpha = alpha

    def _validate_columns(self, columns):
        """
        Validates that the specified columns exist in the dataframe.
        """
        missing_cols = [col for col in columns if col not in self.dataframe.columns]
        if missing_cols:
            raise ValueError(f"Columns {missing_cols} not found in the dataframe.")

    def t_test(
        self, time_column, group_column, control_name, variant_name, metric_column
    ):
        """
        Performs a t-test between a control and variant group on a specified metric and plots the results.

        Parameters:
            time_column (str): The column representing time.
            group_column (str): The column representing the groups (control and variant).
            control_name (str): The name of the control group.
            variant_name (str): The name of the variant group.
            metric_column (str): The metric on which the t-test will be performed.

        Returns:
            pd.DataFrame: Summary statistics and t-test results.
        """
        # Validate columns
        self._validate_columns([time_column, group_column, metric_column])

        # Split the dataframe into control and variant
        control_data = self.dataframe[self.dataframe[group_column] == control_name]
        variant_data = self.dataframe[self.dataframe[group_column] == variant_name]

        # Merge the control and variant data on the time column
        merged = pd.merge(
            control_data,
            variant_data,
            on=time_column,
            suffixes=("_control", "_variant"),
        )

        # Calculate statistics for both groups
        control_stats = control_data[metric_column].describe()
        variant_stats = variant_data[metric_column].describe()

        # Perform t-test
        t_stat, p_value = stats.ttest_ind(
            control_data[metric_column], variant_data[metric_column], equal_var=False
        )
        significant = p_value < self.alpha

        # Calculate additional business metrics
        control_sum = control_data[metric_column].sum()
        variant_sum = variant_data[metric_column].sum()
        total_incrementality = variant_sum - control_sum
        percentual_incrementality = (variant_sum - control_sum) / control_sum

        # Calculate mean incrementality across all timestamps
        merged["incrementality"] = (
            merged[metric_column + "_variant"] - merged[metric_column + "_control"]
        )
        merged["percentual_incrementality"] = (
            merged[metric_column + "_variant"] - merged[metric_column + "_control"]
        ) / merged[metric_column + "_control"]
        mean_incrementality = merged["incrementality"].mean()
        mean_percentual_incrementality = merged["percentual_incrementality"].mean()

        # Calculate minimum non-null dates
        min_date_control = (
            control_data[time_column].loc[control_data[metric_column].notna()].min()
        )
        min_date_variant = (
            variant_data[time_column].loc[variant_data[metric_column].notna()].min()
        )

        # Calculate maximum non-null dates
        max_date_control = (
            control_data[time_column].loc[control_data[metric_column].notna()].max()
        )
        max_date_variant = (
            variant_data[time_column].loc[variant_data[metric_column].notna()].max()
        )

        # Count null values
        null_count_control = control_data[metric_column].isna().sum()
        null_count_variant = variant_data[metric_column].isna().sum()

        # Create a summary dataframe with the results
        summary_df = pd.DataFrame(
            {
                "Group": ["Control", "Variant"],
                "Mean": [control_stats["mean"], variant_stats["mean"]],
                "Min": [control_stats["min"], variant_stats["min"]],
                "Max": [control_stats["max"], variant_stats["max"]],
                "Std": [control_stats["std"], variant_stats["std"]],
                "Total": [control_sum, variant_sum],
                "Min Non-Null Date": [min_date_control, min_date_variant],
                "Max Non-Null Date": [max_date_control, max_date_variant],
                "Null Count": [null_count_control, null_count_variant],
            }
        )

        summary_df["Total Incrementality"] = total_incrementality
        summary_df["Percentual Incrementality (1.0)"] = percentual_incrementality
        summary_df["Mean Incrementality"] = mean_incrementality
        summary_df["Mean Percentual Incrementality (1.0)"] = (
            mean_percentual_incrementality
        )
        summary_df["t-statistic"] = t_stat
        summary_df["p-value"] = p_value
        summary_df["Significant"] = significant

        # Plot the results using DataFrameVisualizer
        v = DataFrameVisualizer(merged)
        v.multiple_variable_lineplot(
            x_column=time_column,
            y_columns=[metric_column + "_control", metric_column + "_variant"],
            colors=["blue", "orange"],
        )

        return summary_df
