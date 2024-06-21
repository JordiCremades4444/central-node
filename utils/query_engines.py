"""Class to extract data from databases
"""

import concurrent.futures
import json
import matplotlib.pyplot as plt
import mysql.connector
import numpy as np
import os
import pandas as pd
import time
import trino

from datetime import datetime
from sqlalchemy import create_engine


class QueryEngines:
    def __init__(self):
        """
        Initializes the QueryEngines object, setting up credentials and connections.
        """
        try:
            self.shared_input = (
                "C:/Users/Jordi Cremades/Documents/Repos/central-node/credentials.json"
            )

            # Load credentials
            with open(self.shared_input, "r") as f:
                self.credentials = json.load(f)

            # Set default paths
            self.to_load_path = os.path.join(os.getcwd(), "to_load")
            self.query_logs_path = os.path.join(os.getcwd(), "query_logs")

            # Ensure output directories exist
            if not os.path.exists(self.to_load_path):
                os.mkdir(self.to_load_path)

            if not os.path.exists(self.query_logs_path):
                os.mkdir(self.query_logs_path)

        except Exception as e:
            print(f"An error occurred during initialization: {e}")

    ##################################
    # -- Query Preparation Section --#
    ##################################

    def prepare_query(
        self,
        query_file,
        params=None,
    ):
        """
        Prepares the query by replacing parameters if needed.

        Parameters:
        ------------
        query_file: String
            Name of the SQL query file.
        params: Dictionary
            Dictionary with parameters to be replaced in the query.
        """
        try:
            self.query_file = query_file
            self.params = params

            # Read and prepare query
            self.qpath = os.path.join(os.getcwd(), "queries", self.query_file)
            with open(self.qpath, "r") as f:
                self.read_query = f.read()

            self.tp__read_query = self.replace_params()  # read query and replace params

        except Exception as e:
            print(f"An error occurred while preparing the query: {e}")

    def replace_params(self):
        """
        Replaces parameters in the query with provided values.

        Returns:
        ------------
        String
            Returns the query with parameters replaced.
        """
        try:
            tp__read_query = self.read_query
            # Replace parameters
            if self.params:
                for param in self.params:
                    param_in_sql = "{" + str(param["name"]) + "}"
                    tp__read_query = tp__read_query.replace(
                        param_in_sql, param["value"]
                    )
            return tp__read_query
        except Exception as e:
            print(f"An error occurred while replacing parameters: {e}")

    def lowercasing(self):
        """
        Converts the content of an SQL file to lowercase and saves it back.

        Returns:
        ------------
        None
        """
        try:
            with open(self.qpath, "r") as f:  # Read the SQL file
                sql_content = f.read()
            lowercase_sql_content = sql_content.lower()  # Convert to lowercase
            with open(self.qpath, "w") as f:  # Save the lowercase content
                f.write(lowercase_sql_content)
        except Exception as e:
            print(f"An error occurred while converting to lowercase: {e}")

    ##################################
    # -- CSV Handling Section --#
    ##################################

    def load_from_csv(self, file_name):
        """
        Loads data from a CSV file into a DataFrame.

        Parameters:
        ------------
        file_name: String
            The name of the CSV file to load data from.

        Returns:
        ------------
        pandas.DataFrame
            DataFrame containing the loaded data.
        """
        try:
            file_path = os.path.join(self.to_load_path, "to_load_" + file_name + ".csv")
            if os.path.exists(file_path):
                return pd.read_csv(file_path)
            else:
                raise FileNotFoundError(f"The file {file_path} does not exist.")
        except Exception as e:
            print(f"An error occurred while loading from CSV: {e}")
            return pd.DataFrame()

    def save_to_csv(self, df, file_name):
        """
        Saves a DataFrame to a CSV file.

        Parameters:
        ------------
        df: pandas.DataFrame
            The DataFrame to save.
        file_name: String
            The name of the CSV file to save data to.
        """
        try:
            file_path = os.path.join(self.to_load_path, "to_load_" + file_name + ".csv")
            df.to_csv(file_path, index=False)
        except Exception as e:
            print(f"An error occurred while saving to CSV: {e}")

    ########################################
    # -- Starburst Query Execution Section --#
    ########################################

    def query_run_starburst(
        self, output_file=None, load_from_output_file=None, print_query=False
    ):
        """
        Runs the query on the Starburst database and returns the result in a DataFrame.

        Parameters:
        ------------
        output_file: String
            If not null, the name of the CSV to save the query results.
        load_from_output_file: String
            If not null, the name of the CSV file to load data from instead of executing the query.
        print_query: Bool
            If True, logs the query to a file in the query_logs directory.

        Returns:
        ------------
        pandas.DataFrame
            Returns a DataFrame containing the query results.
        """
        try:
            if load_from_output_file:
                return self.load_from_csv(load_from_output_file)

            if print_query:  # log the query if specified
                self.log_query(self.tp__read_query, "starburst")

            df = pd.DataFrame()

            # Credentials
            USER = self.credentials["starbust_user"]
            HOST = self.credentials["starbust_host"]
            PORT = self.credentials["starbust_port"]

            # Create connection
            conn_details = {
                "host": HOST,
                "port": PORT,
                "user": USER,
                "http_scheme": "https",
                "auth": trino.auth.OAuth2Authentication(),
            }

            # Execute query
            conn = trino.dbapi.connect(**conn_details)
            df = pd.read_sql(self.tp__read_query, conn)
            conn.close()

            # Save output if specified
            if output_file:
                self.save_to_csv(df, output_file)

            return df
        except Exception as e:
            print(f"An error occurred while running the Starburst query: {e}")
            return pd.DataFrame()

    ####################################
    # -- MySQL Query Execution Section --#
    ####################################

    def query_run_livedb(
        self, output_file=None, load_from_output_file=None, print_query=False
    ):
        """
        Runs the query on the LiveDB (MySQL) database and returns the result in a DataFrame.

        Parameters:
        ------------
        output_file: String
            If not null, the name of the CSV to save the query results.
        load_from_output_file: String
            If not null, the name of the CSV file to load data from instead of executing the query.
        print_query: Bool
            If True, logs the query to a file in the query_logs directory.

        Returns:
        ------------
        pandas.DataFrame
            Returns a DataFrame containing the query results.
        """
        try:
            if load_from_output_file:
                return self.load_from_csv(load_from_output_file)

            if print_query:  # log the query if specified
                self.log_query(self.tp__read_query, "livedb")

            df = pd.DataFrame()
            # Credentials
            USER = self.credentials["livedb_user"]
            PW = self.credentials["livedb_pw"]
            HOST = self.credentials["livedb_host"]
            PORT = self.credentials["livedb_port"]
            DB = self.credentials["livedb_database"]

            # Create connection
            conn_details = {
                "host": HOST,
                "port": PORT,
                "user": USER,
                "password": PW,
                "database": DB,
            }

            # Perform the query
            conn = mysql.connector.connect(**conn_details)
            cursor = conn.cursor()
            cursor.execute(self.tp__read_query)
            query_result = cursor.fetchall()

            # Fetch Data
            columns_names = [desc[0] for desc in cursor.description]
            df = pd.DataFrame(query_result, columns=columns_names)

            # Close connection
            conn.close()
            cursor.close()

            # Save output if specified
            if output_file:
                self.save_to_csv(df, output_file)

            return df
        except Exception as e:
            print(f"An error occurred while running the LiveDB query: {e}")
            return pd.DataFrame()

    ####################################
    # -- Query Logging Section --#
    ####################################

    def log_query(self, query, db_type):
        """
        Logs the query to a file in the query_logs directory.

        Parameters:
        ------------
        query: String
            The SQL query to be logged.
        db_type: String
            The type of database being queried (e.g., 'starburst' or 'livedb').
        """
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            log_file = os.path.join(
                self.query_logs_path, f"{db_type}_query_{timestamp}.sql"
            )

            with open(log_file, "w") as f:
                f.write(query)
        except Exception as e:
            print(f"An error occurred while logging the query: {e}")

    ####################################
    # -- Multiple Queries --#
    ####################################

    def multiple_queries(
        self,
        query_file,
        params_file_name,
        parallelize=False,
        store_steps=False,
        sleep=5,
    ):
        """
        Runs multiple queries based on parameters provided in a JSON file using Starburst.

        Parameters:
        ------------
        query_file: String
            Name of the SQL query file.
        params_file_name: String
            Name to the JSON file containing parameters for each query run.
        parallelize: Bool, optional
            If True, executes queries concurrently using ThreadPoolExecutor.
        store_steps: Bool, optional
            If True, stores the results of each query in a separate CSV file in the to_load folder.
        sleep: Int, optional
            Time between parallelizations.

        Returns:
        ------------
        pandas.DataFrame
            Returns a DataFrame containing the combined results of all queries.
        """
        try:
            combined_df = pd.DataFrame()

            params_file_name = params_file_name + ".json"
            with open(os.path.join("params", params_file_name), "r") as f:
                params_data = json.load(f)

            total_queries = len(params_data)
            completed_queries = 0

            def execute_query(key, params_list):
                nonlocal completed_queries
                params = [
                    {"name": param["name"], "value": param["value"]}
                    for param in params_list
                ]

                self.prepare_query(query_file=query_file, params=params)

                self.log_query(self.tp__read_query, "multiple_queries")

                df = self.query_run_starburst(
                    output_file=None, load_from_output_file=None
                )
                df["param_label"] = str(key)

                completed_queries += 1
                progress_percent = int(completed_queries / total_queries * 100)

                print(f"###### SUCCESSFUL RUN {progress_percent}% - Query {key} DONE.")

                # Store the result if store_steps is True
                if store_steps:
                    output_file_name = os.path.join(
                        self.to_load_path, f"query_result_{key}.csv"
                    )
                    df.to_csv(output_file_name, index=False)

                return df

            if parallelize:
                # Concurrent execution setup
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    futures = []
                    for key, params_list in params_data.items():
                        future = executor.submit(execute_query, key, params_list)
                        futures.append(future)

                        # Optional sleep to stagger submissions
                        time.sleep(sleep)

                    # Collect results and print completion messages
                    for future in concurrent.futures.as_completed(futures):
                        try:
                            df = future.result()
                            combined_df = pd.concat(
                                [combined_df, df], ignore_index=True
                            )
                        except Exception as e:
                            print(f"Error executing query: {e}")

            else:
                # Sequential execution
                for key, params_list in params_data.items():
                    df = execute_query(key, params_list)
                    combined_df = pd.concat([combined_df, df], ignore_index=True)

                    # Optional sleep to pace the queries
                    time.sleep(sleep)

            return combined_df

        except Exception as e:
            print(f"An error occurred while running multiple queries: {e}")
            return pd.DataFrame()
