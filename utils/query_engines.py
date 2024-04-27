"""Class to extract data from databases
"""

import inspect
import json
import os
import pandas as pd
import trino
import matplotlib.pyplot as plt
import numpy as np
from sqlalchemy import create_engine
import mysql.connector


class QueryEngines:
    def __init__(
        self,
        query,
        params=None,
        output_file=None,
        load_from_output_file=None,
        printq=None,
    ):
        """
        Description:
        ------------

        The init method of this function prepares the query by replacing parameters if needed and preparing the output file.


        Parameters:
        ------------

        query: String
            Query with no formating to be trimmed
        params: Dictionary
            Dictionary with parameters to be replaced. If none, there will
            be no parameters
        ouptut_file: String
            If not null, contains the name of the csv that will be stored
        load_from_output_file: String
            If not null, contains the name of the csv that will be stored
        printq: Int
            Integer
        """
        try:
            self.shared_input = (
                "C:/Users/Jordi Cremades/Documents/Repos/central-node/credentials.json"
            )
            self.query = query
            self.params = params  # parameters
            self.output_file = output_file
            self.load_from_output_file = load_from_output_file
            self.output_file_path = os.path.join(os.getcwd(), "inputs")
            if self.output_file:
                self.output_file_name = os.path.join(
                    self.output_file_path, "output_" + output_file + ".csv"
                )
            if self.load_from_output_file:
                self.load_from_output_file_name = os.path.join(
                    self.output_file_path,
                    "output_" + self.load_from_output_file + ".csv",
                )

            # read credentials
            with open(self.shared_input, "r") as f:
                self.credentials = json.load(f)
            # read query and params replace
            self.qpath = os.path.join(os.getcwd(), "queries", self.query)
            with open(self.qpath, "r") as f:
                self.read_query = f.read()
            self.tp__read_query = self.replace_params()  # read query and replace params
            if printq:  # optional print query
                print(self.tp__read_query)
            # if the output will be stored check inputs folder and store
            if self.output_file:
                if not os.path.exists(self.output_file_path):
                    os.mkdir(self.output_file_path)
        except Exception as e:
            print(f"An error occurred: {e}")

    def replace_params(self):
        """
        Description:
        ------------

        This method replaces the desired path for the query


        Parameters:
        ------------

        query: String
            Query with no formating to be trimmed
        self.params: Dictionary
            Dictionary with parameters to be replaced

        Returns:
        ------------

        String
            Returns params replaced query
        """
        try:
            tp__read_query = self.read_query
            # replace parameters
            if self.params:
                for param in self.params:
                    param_in_sql = "{" + str(param["name"]) + "}"
                    tp__read_query = tp__read_query.replace(
                        param_in_sql, param["value"]
                    )
            return tp__read_query
        except Exception as e:
            print(f"An error occurred: {e}")

    def query_run_starbust(self):
        """
        Description:
        ------------

        Runs the query and returns it in a dataframe format.


        Parameters:
        ------------

        self.credential: Dictionary
            Dictionary with credentials
        self.tp__read_query: String
            Parameters replaced query
        self.output_file: String
            Path to output of the query

        Returns:
        ------------

        pandas.Dataframe
            Returns df containg the query results
        """
        try:
            df = pd.DataFrame()
            # credentials
            USER = self.credentials["starbust_user"]
            HOST = self.credentials["starbust_host"]
            PORT = self.credentials["starbust_port"]

            # create connection
            conn_details = {
                "host": HOST,
                "port": PORT,
                "user": USER,
                "http_scheme": "https",
                "auth": trino.auth.OAuth2Authentication(),
            }
            # load_from_output_file
            if self.load_from_output_file:
                df = pd.read_csv(self.load_from_output_file_name)
            # execute query
            else:
                conn = trino.dbapi.connect(**conn_details)
                df = pd.read_sql(self.tp__read_query, conn)
                # output
                if self.output_file:
                    df.to_csv(self.output_file_name, index=False)
            return df
        except Exception as e:
            print(f"An error occurred: {e}")

    def query_run_livedb(self):
        """
        Description:
        ------------

        Runs the query and returns it in a dataframe format.


        Parameters:
        ------------

        self.credential: Dictionary
            Dictionary with credentials
        self.tp__read_query: String
            Parameters replaced query
        self.output_file: String
            Path to output of the query

        Returns:
        ------------

        pandas.Dataframe
            Returns df containg the query results
        """
        try:
            # credentials
            USER = self.credentials["livedb_user"]
            PW = self.credentials["livedb_pw"]
            HOST = self.credentials["livedb_host"]
            PORT = self.credentials["livedb_port"]
            DB = self.credentials["livedb_database"]
            # create connection
            conn_details = {
                "host": HOST,
                "port": PORT,
                "user": USER,
                "password": PW,
                "database": DB,
            }
            # load_from_output_file
            if self.load_from_output_file:
                df = pd.read_csv(self.load_from_output_file_name)
            # execute query
            else:
                # Perform the query
                conn = mysql.connector.connect(**conn_details)
                cursor = conn.cursor()
                cursor.execute(self.tp__read_query)
                query_result = cursor.fetchall()
                # Fetch Data
                columns_names = [desc[0] for desc in cursor.description]
                df = pd.DataFrame(query_result, columns=columns_names)
                # close connection
                conn.close()
                cursor.close()
                # output
                if self.output_file:
                    df.to_csv(self.output_file_name, index=False)
            return df
        except Exception as e:
            print(f"An error occurred: {e}")

    def lowercasing(self):
        """
        Description:
        ------------

        Convert the content of an SQL file to lowercase and save it back.


        Parameters:
        ------------

        sql_file_path: String
            Path to the SQL file to be converted to lowercase.

        Returns:
        ------------

        Replaced file
            Replaces the original file lowercasing all the words and with
            the same
        """
        try:
            with open(self.qpath, "r") as f:  # Read the SQL file
                sql_content = f.read()
            lowercase_sql_content = (
                sql_content.lower()
            )  # Convert the content to lowercase
            with open(
                self.qpath, "w"
            ) as f:  # Save the lowercase content back to the file
                f.write(lowercase_sql_content)
        except Exception as e:
            print(f"An error occurred: {e}")
