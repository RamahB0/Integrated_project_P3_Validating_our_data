"""Data Ingestion Module

This module contains functions for ingesting data from various sources, including databases and remote URLs,
and processing it for further analysis. It utilizes SQLAlchemy for database interaction and Pandas for data manipulation.
"""
from sqlalchemy import create_engine, text
import logging
import pandas as pd
# Name our logger so we know that logs from this module come from the data_ingestion module
logger = logging.getLogger('data_ingestion')
# Set a basic logging message up that prints out a timestamp, the name of our logger, and the message
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

### START FUNCTION

"""Data Ingestion Functions

This module provides functions for creating database engines, querying data from databases, and reading data from web-hosted CSV files.
It includes error handling and logging functionalities to ensure robust data ingestion processes.
"""

def create_db_engine(db_path):
    """
    Create a database engine using the provided database path.

    This function creates a SQLAlchemy engine object to connect to the specified database.
    It tests the connection to ensure it is successful and logs appropriate messages.
    
    Parameters:
    db_path (str): The path to the SQLite database file.

    Returns:
    sqlalchemy.engine.Engine: The engine object for the database connection.

    Raises:
    ImportError: If SQLAlchemy is not installed.
    Exception: If there is an error creating the engine.
    """
    try:
        engine = create_engine(db_path)
        # Test connection
        with engine.connect() as conn:
            pass
        # test if the database engine was created successfully
        logger.info("Database engine created successfully.")
        return engine # Return the engine object if it all works well
    except ImportError: #If we get an ImportError, inform the user SQLAlchemy is not installed
        logger.error("SQLAlchemy is required to use this function. Please install it first.")
        raise e
    except Exception as e:# If we fail to create an engine inform the user
        logger.error(f"Failed to create database engine. Error: {e}")
        raise e
    
def query_data(engine, sql_query):
    """
    Query data from the database using the provided engine and SQL query.

    This function executes the SQL query on the database engine and returns the result as a DataFrame.
    It logs messages for successful execution and error handling.

    Parameters:
    engine (sqlalchemy.engine.Engine): The SQLAlchemy engine object.
    sql_query (str): The SQL query to execute.

    Returns:
    pandas.DataFrame: The result of the SQL query as a DataFrame.

    Raises:
    ValueError: If the query returns an empty DataFrame.
    Exception: If there is an error executing the query.
    """
    try:
        with engine.connect() as connection:
            df = pd.read_sql_query(text(sql_query), connection)
        if df.empty:
            # Log a message or handle the empty DataFrame scenario as needed
            msg = "The query returned an empty DataFrame."
            logger.error(msg)
            raise ValueError(msg)
        logger.info("Query executed successfully.")
        return df
    except ValueError as e: 
        logger.error(f"SQL query failed. Error: {e}")
        raise e
    except Exception as e:
        logger.error(f"An error occurred while querying the database. Error: {e}")
        raise e
    
def read_from_web_CSV(URL):
    """
    Read data from a CSV file located at the provided URL.

    This function reads the CSV file from the specified URL using Pandas.
    It logs messages for successful file reading and error handling.

    Parameters:
    URL (str): The URL of the CSV file to read.

    Returns:
    pandas.DataFrame: The contents of the CSV file as a DataFrame.

    Raises:
    pd.errors.EmptyDataError: If the URL does not point to a valid CSV file.
    Exception: If there is an error reading the CSV file from the web.
    """
    try:
        df = pd.read_csv(URL)
        logger.info("CSV file read successfully from the web.")
        return df
    except pd.errors.EmptyDataError as e:
        logger.error("The URL does not point to a valid CSV file. Please check the URL and try again.")
        raise e
    except Exception as e:
        logger.error(f"Failed to read CSV from the web. Error: {e}")
        raise e
    
### END FUNCTION