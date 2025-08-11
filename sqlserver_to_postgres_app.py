import streamlit as st
import os
import pandas as pd
import threading
import time
import sys
import re
from pyodbc import OperationalError as PyodbcOperationalError, ProgrammingError as PyodbcProgrammingError

# We use different libraries for each database type
try:
    import pyodbc
except ImportError:
    st.error("❌ The required Python library 'pyodbc' is not installed for SQL Server. Please run `pip install pyodbc` from your terminal.")
    pyodbc = None
    
try:
    import psycopg2
except ImportError:
    st.error("❌ The required Python library 'psycopg2' is not installed for PostgreSQL. Please run `pip install psycopg2` from your terminal.")
    psycopg2 = None

# ==============================================================================
#      _           _        _
# | |__   ___| |_ _   _ __ _| |_ ___| |__
# | '_ \/ _ \ __| | | |/ _` | __/ __| '_ \
# | |_) |  __/ |_| |_| | (_| | | |__ \ | | |
# |_.__/\___|\__|\__|___/_| |_|
#
# Edit these values to configure your application
# ==============================================================================
# Use a default config, but the Streamlit form will override this
CONFIG = {
    "DB_TYPE": "SQL Server", # Default to SQL Server
    "SQL_SERVER_NAME": r"DESKTOP-DG1Q26L\SQLEXPRESS",
    "SQL_SOURCE_DB_NAME": "JSCPL",
    "SQL_DEST_DB_NAME": "PivotedDataDB",
    "SQL_TABLE_NAME_RAW": "dbo.FloatTable",
    "SQL_TABLE_NAME_PIVOTED": "dbo.PivotedData",
    "SQL_USERNAME": "",
    "SQL_PASSWORD": "",
    
    "PG_HOST": "localhost",
    "PG_PORT": "5432",
    "PG_SOURCE_DB_NAME": "JSCPL_PG",
    "PG_DEST_DB_NAME": "PivotedDataDB_PG",
    "PG_TABLE_NAME_RAW": "public.FloatTable",
    "PG_TABLE_NAME_PIVOTED": "public.PivotedData",
    "PG_USERNAME": "postgres",
    "PG_PASSWORD": ""
}

TAG_MAPPING = {
    # Existing tags
    251: "TI-31", 253: "TI-32", 254: "TI-33", 255: "TI-35", 256: "TI-35-A",
    257: "TI-36", 258: "TI-37", 259: "TI-38", 260: "TI-39", 261: "TI-40",
    270: "TI-41", 271: "TI-42", 273: "TI-43", 274: "TI-44", 275: "TI-45",
    272: "TI-42A", 279: "TI-54", 199: "TI-107", 201: "TI-109", 296: "TI-73A",
    297: "TI-73B", 280: "TI-55", 154: "PTT-03", 149: "PTB-03", 122: "LT-O5",
    123: "LT-06", 28: "FT-01", 46: "FT-07", 63: "FT-10", 37: "FT-04",

    # New tags based on your screenshot
    317: "TI-317", 316: "TI-316", 315: "TI-315", 314: "TI-314", 313: "TI-313",
    312: "TI-312", 311: "TI-311", 310: "TI-310", 309: "TI-309", 308: "TI-308",

    # Additional tags from your new screenshots
    318: "TI-318", 319: "TI-319", 320: "TI-320", 321: "TI-321", 322: "TI-322",
    323: "TI-323"
}

# ==============================================================================
#   _   _        _
# | | | | __ _ _ __ | | __
# | |_| |/ _` | '_ \| |/ /
# |  _  | (_| | | | |  <
# |_| |_|\__,_|\__|_|_|\_\
#
#   Streamlit App Layout and Logic
# ==============================================================================

# Global State for Streamlit session
if 'sync_running' not in st.session_state:
    st.session_state.sync_running = False
if 'sync_thread' not in st.session_state:
    st.session_state.sync_thread = None
if 'sync_log' not in st.session_state:
    st.session_state.sync_log = []
if 'last_sync_time' not in st.session_state:
    st.session_state.last_sync_time = "Never"

# Streamlit Page Config
st.set_page_config(page_title="SCADA SQL Data Pivoting Tool", layout="centered")
st.title("🔄 SCADA SQL Data Pivoting Tool")
st.markdown("---")

# ---------------------------
# Sidebar Input Form - Displaying Config
# ---------------------------
st.sidebar.header("🔧 Sync Settings")
with st.sidebar.form("connection_form"):
    db_type = st.radio("Select Database Type", ("SQL Server", "PostgreSQL"))
    
    if db_type == "SQL Server":
        st.subheader("SQL Server Details")
        sql_server = st.text_input("SQL Server Name", value=CONFIG["SQL_SERVER_NAME"], help="The server where your SCADA data is located.")
        st.markdown("---")
        st.subheader("Source Details")
        sql_source_db_name = st.text_input("Source Database Name", value=CONFIG["SQL_SOURCE_DB_NAME"], help="The name of the database that contains the raw data.")
        sql_table_name_raw = st.text_input("Source Raw Table Name", value=CONFIG["SQL_TABLE_NAME_RAW"], help="The name of the raw data table.")
        st.markdown("---")
        st.subheader("Destination Details")
        sql_dest_db_name = st.text_input("Destination Database Name", value=CONFIG["SQL_DEST_DB_NAME"], help="The name of the database to use for the pivoted data.")
        sql_table_name_pivoted = st.text_input("Destination Pivoted Table Name", value=CONFIG["SQL_TABLE_NAME_PIVOTED"], help="The name of the table to store the pivoted data.")
        st.markdown("---")
        st.subheader("Authentication")
        use_sql_auth = st.checkbox("Use SQL Server Authentication", value=False, help="Use a username and password instead of Windows auth.")
        if use_sql_auth:
            sql_username = st.text_input("SQL Username", value=CONFIG["SQL_USERNAME"])
            sql_password = st.text_input("SQL Password", value=CONFIG["SQL_PASSWORD"], type="password")
        else:
            sql_username = CONFIG["SQL_USERNAME"]
            sql_password = CONFIG["SQL_PASSWORD"]
    else: # PostgreSQL
        st.subheader("PostgreSQL Details")
        pg_host = st.text_input("PostgreSQL Host", value=CONFIG["PG_HOST"])
        pg_port = st.text_input("PostgreSQL Port", value=CONFIG["PG_PORT"])
        st.markdown("---")
        st.subheader("Source Details")
        pg_source_db_name = st.text_input("Source Database Name", value=CONFIG["PG_SOURCE_DB_NAME"])
        pg_table_name_raw = st.text_input("Source Raw Table Name", value=CONFIG["PG_TABLE_NAME_RAW"])
        st.markdown("---")
        st.subheader("Destination Details")
        pg_dest_db_name = st.text_input("Destination Database Name", value=CONFIG["PG_DEST_DB_NAME"])
        pg_table_name_pivoted = st.text_input("Destination Pivoted Table Name", value=CONFIG["PG_TABLE_NAME_PIVOTED"])
        st.markdown("---")
        st.subheader("Authentication")
        pg_username = st.text_input("PostgreSQL Username", value=CONFIG["PG_USERNAME"])
        pg_password = st.text_input("PostgreSQL Password", value=CONFIG["PG_PASSWORD"], type="password")

    submitted = st.form_submit_button("✅ Save Settings and Initialize")

# ---------------------------
# DB Utility Functions
# ---------------------------

def _get_sql_connection_string(server, db_name, username="", password=""):
    """Builds a connection string for SQL Server."""
    if username and password:
        return f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={db_name};UID={username};PWD={password};'
    else:
        return f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={db_name};Trusted_Connection=yes;'

def _get_pg_connection_string(host, port, db_name, user, password):
    """Builds a connection string for PostgreSQL."""
    return f"host={host} port={port} dbname={db_name} user={user} password={password}"

def create_sqlserver_database_if_not_exists(server, db_name, username, password):
    # (Existing SQL Server DB creation code)
    conn = None
    try:
        conn_str = _get_sql_connection_string(server, "master", username, password)
        conn = pyodbc.connect(conn_str, autocommit=True)
        cursor = conn.cursor()
        check_query = f"SELECT name FROM sys.databases WHERE name = N'{db_name}'"
        cursor.execute(check_query)
        if cursor.fetchone():
            st.info(f"ℹ️ SQL Server database `{db_name}` already exists. Skipping creation.")
            return True
        create_query = f"CREATE DATABASE [{db_name}]"
        st.info(f"ℹ️ Attempting to create SQL Server database `{db_name}`...")
        cursor.execute(create_query)
        st.success(f"✅ SQL Server database `{db_name}` created successfully.")
        return True
    except PyodbcOperationalError as e:
        st.error(f"❌ Could not connect to SQL Server to create the database. Error: {e}")
        _log_message(f"❌ Error creating database: {e}")
        return False
    except Exception as e:
        st.error(f"❌ An error occurred during database creation. Error: {e}")
        _log_message(f"❌ Error creating database: {e}")
        return False
    finally:
        if conn:
            cursor.close()
            conn.close()

def create_pg_database_if_not_exists(host, port, db_name, user, password):
    """Connects to a PostgreSQL server and creates a database if it doesn't exist."""
    conn = None
    try:
        # Connect to the default 'postgres' database to create a new one
        conn_str = _get_pg_connection_string(host, port, "postgres", user, password)
        conn = psycopg2.connect(conn_str)
        conn.autocommit = True
        cursor = conn.cursor()
        
        # Check if database exists
        check_query = f"SELECT 1 FROM pg_database WHERE datname = '{db_name}'"
        cursor.execute(check_query)
        if cursor.fetchone():
            st.info(f"ℹ️ PostgreSQL database `{db_name}` already exists. Skipping creation.")
            return True
        
        # Create the new database
        create_query = f"CREATE DATABASE \"{db_name}\""
        st.info(f"ℹ️ Attempting to create PostgreSQL database `{db_name}`...")
        cursor.execute(create_query)
        st.success(f"✅ PostgreSQL database `{db_name}` created successfully.")
        return True
    except psycopg2.OperationalError as e:
        st.error(f"❌ Could not connect to PostgreSQL to create the database. Error: {e}")
        _log_message(f"❌ Error creating database: {e}")
        return False
    except Exception as e:
        st.error(f"❌ An error occurred during database creation. Error: {e}")
        _log_message(f"❌ Error creating database: {e}")
        return False
    finally:
        if conn:
            cursor.close()
            conn.close()

def create_sql_pivoted_table_if_not_exists(server, db_name, table_name, username, password):
    # (Existing SQL Server table creation code)
    conn = None
    try:
        conn_str = _get_sql_connection_string(server, db_name, username, password)
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        columns = ", ".join([f'[{tag_name}] FLOAT' for tag_name in TAG_MAPPING.values()])
        create_query = f"""
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='{table_name.split('.')[-1]}' and xtype='U')
        CREATE TABLE {table_name} (
            DateAndTime DATETIME PRIMARY KEY,
            {columns}
        );
        """
        st.info(f"ℹ️ Attempting to create pivoted data table `{table_name}`...")
        cursor.execute(create_query)
        conn.commit()
        st.success(f"✅ Pivoted data table `{table_name}` verified/created successfully.")
        return True
    except PyodbcOperationalError as e:
        st.error(f"❌ Could not connect to SQL Server to create the pivoted table. Error: {e}")
        _log_message(f"❌ Error creating table: {e}")
        return False
    except Exception as e:
        st.error(f"❌ An error occurred during pivoted table creation. Error: {e}")
        _log_message(f"❌ Error creating table: {e}")
        return False
    finally:
        if conn:
            cursor.close()
            conn.close()

def create_pg_pivoted_table_if_not_exists(host, port, db_name, user, password, table_name):
    """Connects to PostgreSQL and creates a pivoted table if it doesn't exist."""
    conn = None
    try:
        conn_str = _get_pg_connection_string(host, port, db_name, user, password)
        conn = psycopg2.connect(conn_str)
        cursor = conn.cursor()
        
        # Dynamically build the columns
        columns = ", ".join([f'"{tag_name}" FLOAT' for tag_name in TAG_MAPPING.values()])
        create_query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            "DateAndTime" TIMESTAMP PRIMARY KEY,
            {columns}
        );
        """
        st.info(f"ℹ️ Attempting to create pivoted data table `{table_name}`...")
        cursor.execute(create_query)
        conn.commit()
        st.success(f"✅ Pivoted data table `{table_name}` verified/created successfully.")
        return True
    except psycopg2.OperationalError as e:
        st.error(f"❌ Could not connect to PostgreSQL to create the pivoted table. Error: {e}")
        _log_message(f"❌ Error creating table: {e}")
        return False
    except Exception as e:
        st.error(f"❌ An error occurred during pivoted table creation. Error: {e}")
        _log_message(f"❌ Error creating table: {e}")
        return False
    finally:
        if conn:
            cursor.close()
            conn.close()

def get_latest_sql_timestamp(server, db_name, table_name, username, password):
    # (Existing SQL Server timestamp function)
    conn = None
    try:
        conn_str = _get_sql_connection_string(server, db_name, username, password)
        conn = pyodbc.connect(conn_str)
        query = f"SELECT MAX(DateAndTime) FROM {table_name};"
        df = pd.read_sql(query, conn)
        if df.iloc[0, 0] is None:
            return pd.Timestamp('1970-01-01')
        return df.iloc[0, 0]
    except (PyodbcOperationalError, PyodbcProgrammingError) as e:
        return None
    except Exception as e:
        _log_message(f"❌ An unexpected error occurred while fetching the latest timestamp from SQL Server. Error: {e}")
        return None
    finally:
        if conn:
            conn.close()

def get_latest_pg_timestamp(host, port, db_name, user, password, table_name):
    """Fetches the most recent timestamp from a PostgreSQL database."""
    conn = None
    try:
        conn_str = _get_pg_connection_string(host, port, db_name, user, password)
        conn = psycopg2.connect(conn_str)
        query = f'SELECT MAX("DateAndTime") FROM {table_name};'
        df = pd.read_sql(query, conn)
        if df.iloc[0, 0] is None:
            return pd.Timestamp('1970-01-01')
        return df.iloc[0, 0]
    except (psycopg2.OperationalError, psycopg2.ProgrammingError) as e:
        return None
    except Exception as e:
        _log_message(f"❌ An unexpected error occurred while fetching the latest timestamp from PostgreSQL. Error: {e}")
        return None
    finally:
        if conn:
            conn.close()

def _log_message(message):
    """Thread-safe logging to a Streamlit session state variable."""
    st.session_state.sync_log.append(f"[{pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}] {message}")

# ---------------------------
# Sync Function (Updated for dual DB support)
# ---------------------------
def sync_continuously(config):
    """The main continuous sync loop, now with dual database support."""
    _log_message(f"Starting continuous sync process for {config['DB_TYPE']}...")
    
    while st.session_state.sync_running:
        try:
            latest_timestamp = None
            if config["DB_TYPE"] == "SQL Server":
                latest_timestamp = get_latest_sql_timestamp(config['SQL_SERVER_NAME'], config['SQL_DEST_DB_NAME'], config['SQL_TABLE_NAME_PIVOTED'], config['SQL_USERNAME'], config['SQL_PASSWORD']) or pd.Timestamp('1970-01-01')
            elif config["DB_TYPE"] == "PostgreSQL":
                latest_timestamp = get_latest_pg_timestamp(config['PG_HOST'], config['PG_PORT'], config['PG_DEST_DB_NAME'], config['PG_USERNAME'], config['PG_PASSWORD'], config['PG_TABLE_NAME_PIVOTED']) or pd.Timestamp('1970-01-01')

            _log_message(f"ℹ️ Latest pivoted timestamp in destination DB is: `{latest_timestamp}`.")

            raw_df = pd.DataFrame()
            if config["DB_TYPE"] == "SQL Server":
                # Connect to SQL Server and fetch data
                conn_str_read = _get_sql_connection_string(config["SQL_SERVER_NAME"], config["SQL_SOURCE_DB_NAME"], config['SQL_USERNAME'], config['SQL_PASSWORD'])
                with pyodbc.connect(conn_str_read) as sql_conn_read:
                    sql_query = f"SELECT DateAndTime, TagIndex, Val FROM {config['SQL_TABLE_NAME_RAW']} WHERE DateAndTime > ? ORDER BY DateAndTime ASC;"
                    raw_df = pd.read_sql(sql_query, sql_conn_read, params=[latest_timestamp])
            elif config["DB_TYPE"] == "PostgreSQL":
                # Connect to PostgreSQL and fetch data
                conn_str_read = _get_pg_connection_string(config["PG_HOST"], config["PG_PORT"], config["PG_SOURCE_DB_NAME"], config["PG_USERNAME"], config["PG_PASSWORD"])
                with psycopg2.connect(conn_str_read) as pg_conn_read:
                    sql_query = f'SELECT "DateAndTime", "TagIndex", "Val" FROM {config["PG_TABLE_NAME_RAW"]} WHERE "DateAndTime" > %s ORDER BY "DateAndTime" ASC;'
                    raw_df = pd.read_sql(sql_query, pg_conn_read, params=[latest_timestamp])

            _log_message(f"📁 Fetched {len(raw_df)} new raw rows from source DB.")

            if raw_df.empty:
                _log_message("📁 No new data found. Waiting for new data...")
            else:
                # Pivoting logic remains the same for both DBs, as it's a Pandas operation
                raw_df['DateAndTime'] = pd.to_datetime(raw_df['DateAndTime']).dt.floor('T')
                raw_df = raw_df.sort_values(by='DateAndTime')
                raw_df.drop_duplicates(subset=['DateAndTime', 'TagIndex'], keep='first', inplace=True)
                raw_df['TagIndex'] = raw_df['TagIndex'].map(TAG_MAPPING)
                pivoted_df = raw_df.pivot_table(index='DateAndTime', columns='TagIndex', values='Val', aggfunc='first').reset_index()
                
                _log_message(f"🔄 Pivoted data for timestamps up to: `{pivoted_df['DateAndTime'].max()}`.")
                
                tag_columns = list(TAG_MAPPING.values())
                missing_cols = set(tag_columns) - set(pivoted_df.columns)
                for col in missing_cols:
                    pivoted_df[col] = None
                pivoted_df = pivoted_df[['DateAndTime'] + tag_columns]

                _log_message(f"ℹ️ Inserting {len(pivoted_df)} new pivoted rows into destination DB.")
                
                if config["DB_TYPE"] == "SQL Server":
                    # SQL Server insertion
                    conn_str_write = _get_sql_connection_string(config["SQL_SERVER_NAME"], config["SQL_DEST_DB_NAME"], config['SQL_USERNAME'], config['SQL_PASSWORD'])
                    with pyodbc.connect(conn_str_write, autocommit=True) as conn_write:
                        cursor = conn_write.cursor()
                        columns = ", ".join([f'[{col}]' for col in pivoted_df.columns])
                        placeholders = ", ".join(['?'] * len(pivoted_df.columns))
                        insert_query = f"INSERT INTO {config['SQL_TABLE_NAME_PIVOTED']} ({columns}) VALUES ({placeholders});"
                        rows_to_insert = [tuple(row) for row in pivoted_df.itertuples(index=False, name=None)]
                        if rows_to_insert:
                           # Check for existing timestamps to avoid key violation errors
                            existing_timestamps = []
                            min_ts = pivoted_df['DateAndTime'].min()
                            max_ts = pivoted_df['DateAndTime'].max()
                            check_query = f"SELECT DateAndTime FROM {config['SQL_TABLE_NAME_PIVOTED']} WHERE DateAndTime BETWEEN ? AND ?;"
                            cursor.execute(check_query, (min_ts, max_ts))
                            existing_timestamps = {row[0] for row in cursor.fetchall()}
                            new_rows_to_insert = [row for row in rows_to_insert if row[0] not in existing_timestamps]
                            if new_rows_to_insert:
                                cursor.executemany(insert_query, new_rows_to_insert)
                                conn_write.commit()
                                _log_message(f"✅ Synced {len(new_rows_to_insert)} new pivoted row(s) to destination SQL Server.")
                            else:
                                _log_message("ℹ️ No new pivoted rows to insert after checking for existing data.")

                elif config["DB_TYPE"] == "PostgreSQL":
                    # PostgreSQL insertion
                    conn_str_write = _get_pg_connection_string(config["PG_HOST"], config["PG_PORT"], config["PG_DEST_DB_NAME"], config["PG_USERNAME"], config["PG_PASSWORD"])
                    with psycopg2.connect(conn_str_write) as conn_write:
                        cursor = conn_write.cursor()
                        columns = ", ".join([f'"{col}"' for col in pivoted_df.columns])
                        placeholders = ", ".join(['%s'] * len(pivoted_df.columns))
                        insert_query = f"INSERT INTO {config['PG_TABLE_NAME_PIVOTED']} ({columns}) VALUES ({placeholders}) ON CONFLICT (\"DateAndTime\") DO NOTHING;"
                        rows_to_insert = [tuple(row) for row in pivoted_df.itertuples(index=False, name=None)]
                        if rows_to_insert:
                            cursor.executemany(insert_query, rows_to_insert)
                            conn_write.commit()
                            _log_message(f"✅ Synced {len(rows_to_insert)} new pivoted row(s) to destination PostgreSQL.")


            st.session_state.last_sync_time = pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')

        except Exception as e:
            _log_message(f"❌ A general error occurred: {e}. Stopping sync.")
            st.error(f"❌ A general error occurred: {e}. Stopping sync.")
            st.session_state.sync_running = False
        finally:
            # Connections are managed by 'with' statements, so no need for manual closing here
            pass

        if st.session_state.sync_running:
            _log_message("💤 Waiting for 60 seconds before the next sync cycle...")
            time.sleep(60)

# ==============================================================================
# Main App Flow
# ==============================================================================
# --- Dependency Check ---
if pyodbc is None or psycopg2 is None or pd is None:
    st.stop()


# --- DB Setup ---
if submitted:
    if db_type == "SQL Server":
        if create_sqlserver_database_if_not_exists(sql_server, sql_dest_db_name, sql_username, sql_password):
            create_sql_pivoted_table_if_not_exists(sql_server, sql_dest_db_name, sql_table_name_pivoted, sql_username, sql_password)
            CONFIG['DB_TYPE'] = db_type
            CONFIG['SQL_SERVER_NAME'] = sql_server
            CONFIG['SQL_SOURCE_DB_NAME'] = sql_source_db_name
            CONFIG['SQL_DEST_DB_NAME'] = sql_dest_db_name
            CONFIG['SQL_TABLE_NAME_RAW'] = sql_table_name_raw
            CONFIG['SQL_TABLE_NAME_PIVOTED'] = sql_table_name_pivoted
            CONFIG['SQL_USERNAME'] = sql_username
            CONFIG['SQL_PASSWORD'] = sql_password
    elif db_type == "PostgreSQL":
        if create_pg_database_if_not_exists(pg_host, pg_port, pg_dest_db_name, pg_username, pg_password):
            create_pg_pivoted_table_if_not_exists(pg_host, pg_port, pg_dest_db_name, pg_username, pg_password, pg_table_name_pivoted)
            CONFIG['DB_TYPE'] = db_type
            CONFIG['PG_HOST'] = pg_host
            CONFIG['PG_PORT'] = pg_port
            CONFIG['PG_SOURCE_DB_NAME'] = pg_source_db_name
            CONFIG['PG_DEST_DB_NAME'] = pg_dest_db_name
            CONFIG['PG_TABLE_NAME_RAW'] = pg_table_name_raw
            CONFIG['PG_TABLE_NAME_PIVOTED'] = pg_table_name_pivoted
            CONFIG['PG_USERNAME'] = pg_username
            CONFIG['PG_PASSWORD'] = pg_password
    st.rerun()

# --- Live Data Diagnostic ---
st.header("🩺 Live Data Diagnostic")
st.info("ℹ️ This section shows the most recent timestamp in your raw and pivoted SQL Server databases to confirm the sync is working.")
latest_raw_ts, latest_pivoted_ts = None, None

if CONFIG['DB_TYPE'] == "SQL Server":
    latest_raw_ts = get_latest_sql_timestamp(CONFIG['SQL_SERVER_NAME'], CONFIG['SQL_SOURCE_DB_NAME'], CONFIG['SQL_TABLE_NAME_RAW'], CONFIG['SQL_USERNAME'], CONFIG['SQL_PASSWORD'])
    latest_pivoted_ts = get_latest_sql_timestamp(CONFIG['SQL_SERVER_NAME'], CONFIG['SQL_DEST_DB_NAME'], CONFIG['SQL_TABLE_NAME_PIVOTED'], CONFIG['SQL_USERNAME'], CONFIG['SQL_PASSWORD'])
elif CONFIG['DB_TYPE'] == "PostgreSQL":
    latest_raw_ts = get_latest_pg_timestamp(CONFIG['PG_HOST'], CONFIG['PG_PORT'], CONFIG['PG_SOURCE_DB_NAME'], CONFIG['PG_USERNAME'], CONFIG['PG_PASSWORD'], CONFIG['PG_TABLE_NAME_RAW'])
    latest_pivoted_ts = get_latest_pg_timestamp(CONFIG['PG_HOST'], CONFIG['PG_PORT'], CONFIG['PG_DEST_DB_NAME'], CONFIG['PG_USERNAME'], CONFIG['PG_PASSWORD'], CONFIG['PG_TABLE_NAME_PIVOTED'])
    
if latest_raw_ts:
    st.info(f"✅ The latest timestamp found in the **source raw** table is: `{latest_raw_ts}`.")
else:
    st.warning("⚠️ Could not retrieve the latest timestamp from the source raw table. Check your connection settings.")
st.markdown("---")
if latest_pivoted_ts:
    st.info(f"✅ The latest timestamp found in the **destination pivoted** table is: `{latest_pivoted_ts}`.")
else:
    st.warning("⚠️ The destination pivoted table may be empty or not yet created. The sync process will create it.")

# --- Sync Controls & Log ---
st.header("⚙️ Sync Controls & Status")
col1, col2 = st.columns(2)
if col1.button("🚀 Start Sync") and not st.session_state.sync_running:
    # Validate that all required fields are filled before starting the sync
    if CONFIG['DB_TYPE'] == "SQL Server" and (not CONFIG['SQL_SERVER_NAME'] or not CONFIG['SQL_SOURCE_DB_NAME'] or not CONFIG['SQL_DEST_DB_NAME'] or not CONFIG['SQL_TABLE_NAME_RAW'] or not CONFIG['SQL_TABLE_NAME_PIVOTED']):
        st.error("❌ Please fill in all the required SQL Server details in the sidebar before starting the sync.")
    elif CONFIG['DB_TYPE'] == "PostgreSQL" and (not CONFIG['PG_HOST'] or not CONFIG['PG_PORT'] or not CONFIG['PG_SOURCE_DB_NAME'] or not CONFIG['PG_DEST_DB_NAME'] or not CONFIG['PG_TABLE_NAME_RAW'] or not CONFIG['PG_TABLE_NAME_PIVOTED']):
        st.error("❌ Please fill in all the required PostgreSQL details in the sidebar before starting the sync.")
    else:
        st.session_state.sync_running = True
        st.session_state.sync_log = []
        st.session_state.sync_thread = threading.Thread(target=sync_continuously, args=(CONFIG,), daemon=True)
        st.session_state.sync_thread.start()
        st.info("⏳ Sync started...")

if col2.button("🛑 Stop Sync") and st.session_state.sync_running:
    st.session_state.sync_running = False
    st.warning("🛑 Sync stopped.")

if st.session_state.sync_running:
    st.success("✅ Sync is currently running.")
else:
    st.info("ℹ️ Sync is currently stopped.")

st.markdown("---")
st.subheader(f"📊 Last Successful Sync: {st.session_state.last_sync_time}")
st.subheader("📝 Live Sync Log")
sync_log_container = st.container()
with sync_log_container:
    st.text_area("Log Output", "\n".join(st.session_state.sync_log), height=400)

if st.session_state.sync_running:
    time.sleep(1)
    st.rerun()
