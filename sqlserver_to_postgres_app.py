import json
import time
import os
from typing import List, Dict, Any
from psycopg import connect, sql, OperationalError
import sqlite3

# --- Configuration and Best Practices ---
# Use environment variables for sensitive credentials
POSTGRES_HOST = os.environ.get("PG_HOST", "localhost")
POSTGRES_DB = os.environ.get("PG_DB", "your_database_name")
POSTGRES_USER = os.environ.get("PG_USER", "your_username")
POSTGRES_PASSWORD = os.environ.get("PG_PASSWORD", "your_password")

# The name of the table in your source and destination databases
SOURCE_TABLE = "source_data"
DESTINATION_TABLE = "destination_data"

# The column that holds the tag index for mapping
TAG_INDEX_COLUMN = "tag_index"

# The interval in seconds to wait between sync cycles
SYNC_INTERVAL = 60  # Sync every 1 minute

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
        exit()
    except json.JSONDecodeError:
        print("Error: Invalid JSON in config.json. Please check for syntax errors.")
        exit()

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

def connect_to_source_db():
    """
    Establishes a connection to the source SQL database.
    This example uses an in-memory SQLite database for simplicity.
    In a real-world scenario, you would replace this with your actual database connector.

    Returns:
        sqlite3.Connection: A database connection object, or None if the connection fails.
    """
    try:
        conn = sqlite3.connect(":memory:") # Example for an in-memory db
        # Create a dummy table and some data for demonstration
        cursor = conn.cursor()
        cursor.execute(f"CREATE TABLE IF NOT EXISTS {SOURCE_TABLE} (id INTEGER PRIMARY KEY, {TAG_INDEX_COLUMN} TEXT, value TEXT)")
        cursor.execute(f"INSERT INTO {SOURCE_TABLE} (id, {TAG_INDEX_COLUMN}, value) VALUES (1, '1', 'Initial data point')")
        conn.commit()
        print("Successfully connected to source SQLite DB.")
        return conn
    except sqlite3.OperationalError as e:
        print(f"Error connecting to source database: {e}")
        return None

def get_source_data(conn: sqlite3.Connection) -> List[Dict[str, Any]]:
    """
    Fetches all data from the source database.
    
    Args:
        conn (sqlite3.Connection): The connection object for the source database.
    
    Returns:
        List[Dict[str, Any]]: A list of dictionaries, where each dictionary
                               represents a row of data from the source.
    """
    cursor = conn.cursor()
    cursor.execute(f"SELECT * FROM {SOURCE_TABLE}")
    
    # Get column names to create a dictionary for each row
    columns = [desc[0] for desc in cursor.description]
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
                tag_index = row.get(TAG_INDEX_COLUMN)
                
                # Map the tag index to the full tag name from config.json
                tag_name = tag_mapping.get(tag_index, 'unknown')
                
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
                cursor.execute(upsert_query, (id_value, tag_index, tag_name, other_value))
                print(f"Upserted row with id: {id_value}, mapped tag: {tag_name}")
            
            # Commit the transaction after all upserts are complete
            conn.commit()
            print("Successfully committed changes.")
    except Exception as e:
        print(f"An error occurred during upsert operation: {e}")
        conn.rollback() # Roll back any changes if an error occurred

def main():
    """
    Main function to run the continuous data synchronization process.
    """
    print("Starting continuous data synchronization process...")
    
    # Load configuration once
    config = load_config()
    tag_mapping = config.get("tag_mapping", {})
    
    while True:
        source_conn = None
        postgres_conn = None
        try:
            # Connect to both databases
            source_conn = connect_to_source_db()
            postgres_conn = connect_to_postgres()
            
            if source_conn and postgres_conn:
                print(f"\n[{time.strftime('%Y-%m-%d %H:%M:%S')}] Sync cycle starting...")
                
                # Get data from the source database
                source_data = get_source_data(source_conn)
                
                if not source_data:
                    print("No new data found in source database.")
                else:
                    print(f"Found {len(source_data)} rows to process.")
                    
                    # Upsert the data into PostgreSQL
                    upsert_data_to_postgres(postgres_conn, source_data, tag_mapping)

        except Exception as e:
            print(f"An unexpected error occurred: {e}")
        finally:
            # Ensure database connections are always closed
            if source_conn:
                source_conn.close()
            if postgres_conn:
                postgres_conn.close()
            
            # Wait for the next sync cycle
            print(f"Sync cycle complete. Waiting for {SYNC_INTERVAL} seconds...")
            time.sleep(SYNC_INTERVAL)

if __name__ == "__main__":
    main()
