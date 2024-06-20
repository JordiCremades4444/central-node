"""Class to extract data from databases
"""

import concurrent.futures
import time
import json
import os
import pandas as pd
import trino
import matplotlib.pyplot as plt
import numpy as np
from sqlalchemy import create_engine
import mysql.connector


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
            self.output_file_path = os.path.join(os.getcwd(), "inputs")

            # Ensure output directory exists
            if not os.path.exists(self.output_file_path):
                os.mkdir(self.output_file_path)

        except Exception as e:
            print(f"An error occurred during initialization: {e}")

    def prepare_query(
        self,
        query_file,
        params=None,
        output_file=None,
        load_from_output_file=None,
        printq=None,
    ):
        """
        Prepares the query by replacing parameters if needed and sets up output file paths.

        Parameters:
        ------------
        query_file: String
            Name of the SQL query file.
        params: Dictionary
            Dictionary with parameters to be replaced in the query.
        output_file: String
            If not null, the name of the CSV to save the query results.
        load_from_output_file: String
            If not null, the name of the CSV file to load data from instead of executing the query.
        printq: Int
            If provided, prints the final query.
        """
        try:
            self.query_file = query_file
            self.params = params  # parameters
            self.output_file = output_file
            self.load_from_output_file = load_from_output_file

            if self.output_file:
                self.output_file_name = os.path.join(
                    self.output_file_path, "output_" + self.output_file + ".csv"
                )

            if self.load_from_output_file:
                self.load_from_output_file_name = os.path.join(
                    self.output_file_path,
                    "output_" + self.load_from_output_file + ".csv",
                )

            # Read and prepare query
            self.qpath = os.path.join(os.getcwd(), "queries", self.query_file)
            with open(self.qpath, "r") as f:
                self.read_query = f.read()

            self.tp__read_query = self.replace_params()  # read query and replace params

            if printq:  # optional print query
                print(self.tp__read_query)
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

    def query_run_starbust(self):
        """
        Runs the query on the Starburst database and returns the result in a DataFrame.

        Returns:
        ------------
        pandas.DataFrame
            Returns a DataFrame containing the query results.
        """
        try:
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

            # Load from output file if specified
            if self.load_from_output_file:
                df = pd.read_csv(self.load_from_output_file_name)
            else:
                # Execute query
                conn = trino.dbapi.connect(**conn_details)
                df = pd.read_sql(self.tp__read_query, conn)
                conn.close()

                # Save output if specified
                if self.output_file:
                    df.to_csv(self.output_file_name, index=False)
            return df
        except Exception as e:
            print(f"An error occurred while running the Starburst query: {e}")

    def query_run_livedb(self):
        """
        Runs the query on the LiveDB (MySQL) database and returns the result in a DataFrame.

        Returns:
        ------------
        pandas.DataFrame
            Returns a DataFrame containing the query results.
        """
        try:
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

            # Load from output file if specified
            if self.load_from_output_file:
                df = pd.read_csv(self.load_from_output_file_name)
            else:
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
                if self.output_file:
                    df.to_csv(self.output_file_name, index=False)
            return df
        except Exception as e:
            print(f"An error occurred while running the LiveDB query: {e}")

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

    def multiple_queries(
        self, query_file, params_file_path, parallelize=False, printq=False, sleep=5
    ):
        """
        Runs multiple queries based on parameters provided in a JSON file.

        Parameters:
        ------------
        query_file: String
            Name of the SQL query file.
        params_file_path: String
            Path to the JSON file containing parameters for each query run.
        parallelize: Bool, optional
            If True, executes queries concurrently using ThreadPoolExecutor.
        printq: Bool, optional
            If True, prints each query before execution.
        sleep: Int
            Time between parallelizations

        Returns:
        ------------
        pandas.DataFrame
            Returns a DataFrame containing the combined results of all queries.
        """
        try:
            combined_df = pd.DataFrame()

            with open(params_file_path, "r") as f:
                params_data = json.load(f)

            def execute_query(key, params_list):
                params = [
                    {"name": param["name"], "value": param["value"]}
                    for param in params_list
                ]
                self.prepare_query(query_file=query_file, params=params, printq=printq)
                df = self.query_run_starbust()
                df["param_label"] = str(key)
                return df

            if parallelize:
                # Concurrent execution setup
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    futures = []
                    for key, params_list in params_data.items():
                        futures.append(executor.submit(execute_query, key, params_list))
                        time.sleep(sleep)

                    # Gather results from futures
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
                    params = [
                        {"name": param["name"], "value": param["value"]}
                        for param in params_list
                    ]
                    self.prepare_query(
                        query_file=query_file, params=params, printq=printq
                    )
                    df = self.query_run_starbust()
                    df["param_label"] = str(key)
                    combined_df = pd.concat([combined_df, df], ignore_index=True)
                    time.sleep(sleep)

            return combined_df

        except Exception as e:
            print(f"An error occurred while running multiple queries: {e}")
