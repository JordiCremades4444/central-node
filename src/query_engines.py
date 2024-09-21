import json
import os
import pandas as pd
import mysql.connector
import trino
from datetime import datetime


class QueryEngines:
    """
    A class to manage SQL queries and interactions with databases.

    Attributes:
        shared_input (str): Path to the credentials JSON file.
        to_load_path (str): Directory path for loading data.
        query_logs_path (str): Directory path for query logs.
        credentials (dict): Database credentials loaded from JSON.
        query_file (str): SQL query file name.
        params (list of dict): Query parameters to replace.
        to_load_file (str): CSV file name to save query results.
        load_from_to_load_file (str): CSV file name to load data from.
        read_query (str): The raw SQL query read from file.
        tp__read_query (str): The SQL query with parameters replaced.
    """

    def __init__(self):
        """
        Initializes the QueryEngines object by loading credentials and setting paths.
        """
        self.shared_input = (
            "/Users/jordicremades/Documents/repos/central-node/credentials.json"
        )
        self.to_load_path = os.path.join(os.getcwd(), "to_load")
        self.query_logs_path = os.path.join(os.getcwd(), "query_logs")
        self._load_credentials()

    def _load_credentials(self):
        """
        Loads database credentials from the JSON file.
        """
        with open(self.shared_input, "r") as f:
            self.credentials = json.load(f)

    def _ensure_directory(self, path):
        """
        Ensures that the specified directory exists.

        Parameters:
            path (str): Directory path to check or create.
        """
        if not os.path.exists(path):
            os.makedirs(path)

    def prepare_query(
        self, query_file, params=None, to_load_file=None, load_from_to_load_file=None
    ):
        """
        Prepares the SQL query by reading from a file and replacing parameters.

        Parameters:
            query_file (str): Name of the SQL query file.
            params (list of dict, optional): Parameters to replace in the query.
            to_load_file (str, optional): CSV file name to save query results.
            load_from_to_load_file (str, optional): CSV file name to load data from.
        """
        self.query_file = query_file
        self.params = params
        self.to_load_file = to_load_file
        self.load_from_to_load_file = load_from_to_load_file
        self._ensure_directory(self.to_load_path)
        self._ensure_directory(self.query_logs_path)
        with open(os.path.join(os.getcwd(), "queries", self.query_file), "r") as f:
            self.read_query = f.read()
        self.tp__read_query = self.replace_params()

    def replace_params(self):
        """
        Replaces parameters in the SQL query with provided values.

        Returns:
            str: The SQL query with parameters replaced.
        """
        if self.params:
            for param in self.params:
                self.read_query = self.read_query.replace(
                    f"{{{param['name']}}}", param["value"]
                )
        return self.read_query

    def load_from_csv(self, file_name):
        """
        Loads data from a CSV file into a DataFrame.

        Parameters:
            file_name (str): Name of the CSV file.

        Returns:
            pd.DataFrame: DataFrame containing the loaded data.

        Raises:
            FileNotFoundError: If the CSV file does not exist.
        """
        file_path = os.path.join(self.to_load_path, f"to_load_{file_name}.csv")
        if os.path.exists(file_path):
            return pd.read_csv(file_path)
        raise FileNotFoundError(f"The file {file_path} does not exist.")

    def save_to_csv(self, df, file_name):
        """
        Saves a DataFrame to a CSV file.

        Parameters:
            df (pd.DataFrame): The DataFrame to save.
            file_name (str): Name of the CSV file.
        """
        df.to_csv(
            os.path.join(self.to_load_path, f"to_load_{file_name}.csv"), index=False
        )

    def query_run_starburst(self):
        """
        Runs the SQL query on the Starburst database.

        Returns:
            pd.DataFrame: DataFrame containing the query results.
        """
        if self.load_from_to_load_file:
            return self.load_from_csv(self.load_from_to_load_file)
        self.log_query(self.tp__read_query, "starburst")
        conn_details = {
            "host": self.credentials["starbust_host"],
            "port": self.credentials["starbust_port"],
            "user": self.credentials["starbust_user"],
            "http_scheme": "https",
            "auth": trino.auth.OAuth2Authentication(),
        }
        with trino.dbapi.connect(**conn_details) as conn:
            df = pd.read_sql(self.tp__read_query, conn)
        if self.to_load_file:
            self.save_to_csv(df, self.to_load_file)
        return df

    def query_run_livedb(self):
        """
        Runs the SQL query on the LiveDB (MySQL) database.

        Returns:
            pd.DataFrame: DataFrame containing the query results.
        """
        if self.load_from_to_load_file:
            return self.load_from_csv(self.load_from_to_load_file)
        self.log_query(self.tp__read_query, "livedb")
        conn_details = {
            "host": self.credentials["livedb_host"],
            "port": self.credentials["livedb_port"],
            "user": self.credentials["livedb_user"],
            "password": self.credentials["livedb_pw"],
            "database": self.credentials["livedb_database"],
        }
        with mysql.connector.connect(**conn_details) as conn:
            cursor = conn.cursor()
            cursor.execute(self.tp__read_query)
            columns_names = [desc[0] for desc in cursor.description]
            df = pd.DataFrame(cursor.fetchall(), columns=columns_names)
        if self.to_load_file:
            self.save_to_csv(df, self.to_load_file)
        return df

    def log_query(self, query, db_type):
        """
        Logs the SQL query to a file.

        Parameters:
            query (str): The SQL query to log.
            db_type (str): Type of database (e.g., 'starburst' or 'livedb').
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file = os.path.join(
            self.query_logs_path, f"{db_type}_query_{timestamp}.sql"
        )
        with open(log_file, "w") as f:
            f.write(query)
