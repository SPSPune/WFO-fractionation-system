import json
import time
import os
from typing import List, Dict, Any
from psycopg import connect, sql, OperationalError
import pyodbc  # Using pyodbc for SQL Server as per previous discussion

# --- Configuration and Best Practices ---
# Use environment variables for sensitive credentials.
# Make sure to set these in your system before running the script.
POSTGRES_HOST = os.environ.get("PG_HOST", "localhost")
POSTGRES_DB = os.environ.get("PG_DB", "your_database_name")
POSTGRES_USER = os.environ.get("PG_USER", "your_username")
POSTGRES_PASSWORD = os.environ.get("PG_PASSWORD", "your_password")

# The name of the table in your source and destination databases
# Note: "dbo.FloatTable" is a SQL Server name, so we use it here.
SOURCE_TABLE = "dbo.FloatTable"
DESTINATION_TABLE = "destination_data"

# The column that holds the tag index for mapping
TAG_INDEX_COLUMN = "tag_index"

def load_config() -> Dict[str, Any]:
    """
    Loads the tag mapping from the config.json file.
    
    Returns:
        Dict[str, Any]: The loaded configuration data.
    """
    try:
        with open('config.json', 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        print("Error: config.json file not found. Exiting.")
        return {} # Return an empty dict to prevent crash
    except json.JSONDecodeError:
        print("Error: Invalid JSON in config.json. Please check for syntax errors.")
        return {}

def connect_to_postgres():
    """
    Establishes a connection to the PostgreSQL database.
    
    Returns:
        psycopg.Connection: A database connection object, or None if the connection fails.
    """
    try:
        conn = connect(
            host=POSTGRES_HOST,
            dbname=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD
        )
        print("Successfully connected to PostgreSQL.")
        return conn
    except OperationalError as e:
        print(f"Error connecting to PostgreSQL: {e}")
        return None

def connect_to_source_db(sql_server_config: Dict[str, str]):
    """
    Establishes a connection to the source SQL Server database.
    
    Args:
        sql_server_config (Dict[str, str]): The configuration dictionary for SQL Server.

    Returns:
        pyodbc.Connection: A database connection object, or None if the connection fails.
    """
    try:
        cnxn_string = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={sql_server_config['server']};"
            f"DATABASE={sql_server_config['database']};"
            f"UID={sql_server_config['user']};"
            f"PWD={sql_server_config['password']}"
        )
        conn = pyodbc.connect(cnxn_string)
        print("Successfully connected to source SQL Server DB.")
        return conn
    except Exception as e:
        print(f"Error connecting to source SQL Server database: {e}")
        return None

def get_source_data(conn: pyodbc.Connection) -> List[Dict[str, Any]]:
    """
    Fetches all data from the source database.
    
    Args:
        conn (pyodbc.Connection): The connection object for the source database.
    
    Returns:
        List[Dict[str, Any]]: A list of dictionaries, where each dictionary
                               represents a row of data from the source.
    """
    cursor = conn.cursor()
    cursor.execute(f"SELECT * FROM {SOURCE_TABLE}")
    
    # Get column names to create a dictionary for each row
    columns = [column[0] for column in cursor.description]
    data = [dict(zip(columns, row)) for row in cursor.fetchall()]
    
    return data

def upsert_data_to_postgres(conn: connect, data: List[Dict[str, Any]], tag_mapping: Dict[str, str]):
    """
    Performs an upsert operation (UPDATE or INSERT) on the PostgreSQL table.
    
    Args:
        conn (connect): The connection object for the PostgreSQL database.
        data (List[Dict[str, Any]]): The data to be upserted.
        tag_mapping (Dict[str, str]): The mapping from tag index to tag name.
    """
    try:
        with conn.cursor() as cursor:
            for row in data:
                # Convert the tag index to a string for mapping
                tag_index_str = str(row.get(TAG_INDEX_COLUMN))
                
                # Map the tag index to the full tag name from config.json
                tag_name = tag_mapping.get(tag_index_str, 'unknown')
                
                # Assume a simple schema for the destination table
                # (id, original_tag_index, mapped_tag_name, value)
                id_value = row.get('id')
                other_value = row.get('value')
                
                if id_value is None:
                    print("Skipping row with missing 'id'.")
                    continue
                
                # Construct the SQL query for an upsert operation
                upsert_query = sql.SQL("""
                    INSERT INTO {table} (id, original_tag_index, mapped_tag_name, value)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (id) DO UPDATE
                    SET original_tag_index = EXCLUDED.original_tag_index,
                        mapped_tag_name = EXCLUDED.mapped_tag_name,
                        value = EXCLUDED.value;
                """).format(table=sql.Identifier(DESTINATION_TABLE))
                
                # Execute the query with the mapped data
                cursor.execute(upsert_query, (id_value, tag_index_str, tag_name, other_value))
                print(f"Upserted row with id: {id_value}, mapped tag: {tag_name}")
            
            # Commit the transaction after all upserts are complete
            conn.commit()
            print("Successfully committed changes.")
    except Exception as e:
        print(f"An error occurred during upsert operation: {e}")
        conn.rollback() # Roll back any changes if an error occurred

def run_sync_cycle():
    """
    Performs a single data synchronization cycle.
    """
    print(f"\n[{time.strftime('%Y-%m-%d %H:%M:%S')}] Sync cycle starting...")

    config = load_config()
    tag_mapping = config.get("tag_mapping", {})
    sql_server_config = config.get("sql_server", {})

    source_conn = None
    postgres_conn = None
    try:
        source_conn = connect_to_source_db(sql_server_config)
        postgres_conn = connect_to_postgres()
        
        if source_conn and postgres_conn:
            source_data = get_source_data(source_conn)
            
            if not source_data:
                print("No new data found in source database.")
            else:
                print(f"Found {len(source_data)} rows to process.")
                upsert_data_to_postgres(postgres_conn, source_data, tag_mapping)

    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    finally:
        if source_conn:
            source_conn.close()
        if postgres_conn:
            postgres_conn.close()
    
    print(f"Sync cycle complete.")

# If you want to run this in an interactive window, just call the function.
# This code block will run the sync once when the script is executed.
if __name__ == "__main__":
    run_sync_cycle()
    print("Script finished. To run again in an interactive window, call run_sync_cycle().")
