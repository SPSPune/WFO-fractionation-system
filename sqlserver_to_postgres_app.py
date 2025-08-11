import streamlit as st
import psycopg2
import os
import pandas as pd
import threading
import time
import sys
import pyodbc
import re
from psycopg2.errors import DuplicateDatabase

# ==============================================================================
#      _            _             _      _
# | |__   ___| |_ _   _  __ _| |_ ___| |__
# | '_ \ / _ \ __| | | |/ _` | __/ __| '_ \
# | |_) |  __/ |_| |_| | (_| | | |__ \ | | |
# |_.__/ \___|\__|\__,_|\__,_|\__|___/_| |_|
#
# Edit these values to configure your application
# ==============================================================================
CONFIG = {
    "SQL_SERVER_NAME": r"DESKTOP-DG1Q26L\SQLEXPRESS",
    "SQL_DB_NAME": "JSCPL",
    "SQL_TABLE_NAME": "dbo.FloatTable",
    "PG_HOST": "localhost",
    "PG_PORT": "5432",
    "PG_USER": "postgres",
    "PG_DB_NAME": "scada_data",
    "PG_TABLE_NAME": "scada_data"
}

# The dictionary of TagIndex numbers to their friendly names.
# The sync process will IGNORE any tags from the SQL data that are not in this list.
TAG_MAPPING = {
    # Existing tags
    251: "TI-31", 253: "TI-32", 254: "TI-33", 255: "TI-35", 256: "TI-35-A",
    257: "TI-36", 258: "TI-37", 259: "TI-38", 260: "TI-39", 261: "TI-40",
    270: "TI-41", 271: "TI-42", 273: "TI-43", 274: "TI-44", 275: "TI-45",
    272: "TI-42A", 279: "TI-54", 199: "TI-107", 201: "TI-109", 296: "TI-73A",
    297: "TI-73B", 280: "TI-55", 154: "PTT-03", 149: "PTB-03", 122: "LT-O5",
    123: "LT-06", 28: "FT-01", 46: "FT-07", 63: "FT-10", 37: "FT-04",
    
    # New tags based on your screenshot
    317: "TI-317",
    316: "TI-316",
    315: "TI-315",
    314: "TI-314",
    313: "TI-313",
    312: "TI-312",
    311: "TI-311",
    310: "TI-310",
    309: "TI-309",
    308: "TI-308",
    
    # Additional tags from your new screenshots
    318: "TI-318",
    319: "TI-319",
    320: "TI-320",
    321: "TI-321",
    322: "TI-322",
    323: "TI-323"
}


# ==============================================================================
#  _   _          _
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
st.set_page_config(page_title="SCADA SQL to PostgreSQL Sync", layout="centered")
st.title("🔄 SCADA SQL to PostgreSQL Sync Tool")
st.markdown("---")

# ---------------------------
# Sidebar Input Form - Displaying Config
# ---------------------------
st.sidebar.header("🔧 Sync Settings")
with st.sidebar.form("connection_form"):
    st.subheader("SQL Server Details")
    sql_server = st.text_input("SQL Server Name", value=CONFIG["SQL_SERVER_NAME"], help="The server where your SCADA data is located.")
    sql_db_name = st.text_input("SQL Server Database Name", value=CONFIG["SQL_DB_NAME"], help="The name of the database that contains your SCADA data.")
    sql_table_name = st.text_input("SQL Server Data Table Name", value=CONFIG["SQL_TABLE_NAME"], help="The name of the table that contains your SCADA data.")

    st.subheader("PostgreSQL Details")
    host = st.text_input("Host", value=CONFIG["PG_HOST"], help="The hostname of your PostgreSQL server.")
    port = st.text_input("Port", value=CONFIG["PG_PORT"], help="The port for your PostgreSQL server (default is 5432).")
    user = st.text_input("Username", value=CONFIG["PG_USER"], help="The username for PostgreSQL access.")
    password = st.text_input("Password", type="password", help="The password for the PostgreSQL user.")
    db_name = st.text_input("PostgreSQL Database Name", value=CONFIG["PG_DB_NAME"], help="The name of the database to create or use.")
    pg_table_name = st.text_input("Target Table Name", value=CONFIG["PG_TABLE_NAME"], help="The name of the table to create or use for storing the data.")

    submitted = st.form_submit_button("✅ Save Settings and Initialize")

# ---------------------------
# DB Utility Functions
# ---------------------------
def _is_valid_db_name(name):
    """Checks if a string is a valid PostgreSQL database name."""
    return bool(re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', name))

def create_database_if_not_exists(host, port, user, password, db_name):
    """Handles the creation of the database gracefully."""
    if not _is_valid_db_name(db_name):
        st.error(f"❌ Invalid PostgreSQL database name: `{db_name}`. Please use a simple name with only letters, numbers, and underscores (e.g., `scada_db`).")
        return False
    
    if db_name.lower() == 'postgres':
        st.error(f"❌ The database name `{db_name}` is a reserved name. Please choose a different name for your database.")
        return False

    conn = None
    try:
        conn = psycopg2.connect(host=host, port=port, user=user, password=password, dbname="postgres")
        conn.autocommit = True
        cursor = conn.cursor()
        create_query = f"CREATE DATABASE \"{db_name}\""
        st.info(f"ℹ️ Attempting to create PostgreSQL database `{db_name}`...")
        cursor.execute(create_query)
        st.success(f"✅ PostgreSQL database `{db_name}` created successfully.")
        return True
    
    except DuplicateDatabase:
        st.info(f"ℹ️ PostgreSQL database `{db_name}` already exists. Skipping creation.")
        return True
    except psycopg2.OperationalError as e:
        st.error(f"❌ Cannot connect to PostgreSQL to create the database. Please check your **Host, Port, Username, and Password**. Error: {e}")
        return False
    except Exception as e:
        st.error(f"❌ An unexpected error occurred while creating the PostgreSQL database. Error: {e}")
        return False
    finally:
        if conn:
            conn.close()

def create_pivoted_table_if_not_exists(host, port, user, password, db_name, table_name):
    """Connects to the specific database and creates the target table if it doesn't exist."""
    conn = None
    try:
        conn = psycopg2.connect(host=host, port=port, user=user, password=password, dbname=db_name)
        cursor = conn.cursor()
        columns = ",\n".join([f'"{tag}" FLOAT' for tag in TAG_MAPPING.values()])
        create_query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            DateAndTime TIMESTAMP,
            {columns}
        );
        """
        st.info(f"ℹ️ Attempting to create table `{table_name}`...")
        cursor.execute(create_query)
        conn.commit()
        st.success(f"✅ Table `{table_name}` verified/created successfully.")
        return True
    except psycopg2.OperationalError as e:
        st.error(f"❌ Could not connect to database `{db_name}` to create the table. Please verify the database exists and your credentials are correct. Error: {e}")
        return False
    except Exception as e:
        st.error(f"❌ An error occurred during table creation. Error: {e}")
        return False
    finally:
        if conn:
            cursor.close()
            conn.close()

def get_latest_sql_timestamp(sql_server, sql_db_name, sql_table_name):
    """Fetches the most recent timestamp from the SQL Server database."""
    conn = None
    try:
        conn_str = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={sql_server};DATABASE={sql_db_name};Trusted_Connection=yes;'
        conn = pyodbc.connect(conn_str)
        query = f"SELECT MAX(DateAndTime) FROM {sql_table_name};"
        df = pd.read_sql(query, conn)
        conn.close()
        return df.iloc[0, 0]
    except Exception as e:
        st.error(f"❌ Could not fetch latest timestamp from SQL Server. Error: {e}")
        return None

def _log_message(message):
    """Thread-safe logging to a Streamlit session state variable."""
    st.session_state.sync_log.append(f"[{pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}] {message}")

# ---------------------------
# Sync Function (Diagnostic Mode)
# ---------------------------
def sync_continuously(config, tag_mapping, password):
    """The main continuous sync loop, now with detailed diagnostics."""
    _log_message("Starting continuous sync process...")
    
    while st.session_state.sync_running:
        sql_conn, pg_conn = None, None
        try:
            # Step 1: Connect to SQL Server
            conn_str = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={config["SQL_SERVER_NAME"]};DATABASE={config["SQL_DB_NAME"]};Trusted_Connection=yes;'
            _log_message("ℹ️ Connecting to SQL Server...")
            sql_conn = pyodbc.connect(conn_str)
            _log_message(f"✅ Connected to SQL Server at `{config['SQL_SERVER_NAME']}`.")
            sql_cursor = sql_conn.cursor()

            # Step 2: Get the latest timestamp from PostgreSQL
            _log_message("ℹ️ Connecting to PostgreSQL to get the latest timestamp...")
            pg_conn = psycopg2.connect(host=config['PG_HOST'], port=config['PG_PORT'], user=config['PG_USER'], password=password, dbname=config['PG_DB_NAME'])
            pg_cursor = pg_conn.cursor()
            pg_cursor.execute(f"SELECT MAX(DateAndTime) FROM {config['PG_TABLE_NAME']};")
            latest_timestamp = pg_cursor.fetchone()[0]
            
            if latest_timestamp:
                _log_message(f"ℹ️ Latest timestamp in PostgreSQL is: `{latest_timestamp}`.")
            else:
                latest_timestamp = '1970-01-01'
                _log_message(f"ℹ️ PostgreSQL table is empty. Starting sync from: `{latest_timestamp}`.")

            # Step 3: Query SQL Server for new data
            sql_query = f"""
            SELECT DateAndTime, TagIndex, Val
            FROM {config['SQL_TABLE_NAME']}
            WHERE DateAndTime > ?
            ORDER BY DateAndTime ASC;
            """
            _log_message(f"ℹ️ Fetching new data from SQL Server with the query: `WHERE DateAndTime > {latest_timestamp}`.")
            sql_cursor.execute(sql_query, latest_timestamp)
            rows = sql_cursor.fetchall()
            _log_message(f"📁 Fetched {len(rows)} rows from SQL Server.")

            if not rows:
                _log_message("📁 No new data found in SQL Server. Waiting for new data...")
            else:
                # Step 4: Process and Filter the new data
                df = pd.DataFrame(rows, columns=["DateAndTime", "TagIndex", "Val"])
                
                # Convert TagIndex to int to match the dictionary keys
                df['TagIndex'] = pd.to_numeric(df['TagIndex'], errors='coerce').astype('Int64')
                
                _log_message(f"ℹ️ Now filtering for relevant tags using the `TAG_MAPPING`.")
                df["TAG"] = df["TagIndex"].map(tag_mapping)
                # Drop rows where 'TAG' is NaN (i.e., not in our tag_mapping)
                df.dropna(subset=["TAG"], inplace=True)
                
                rows_after_filter = len(df)
                _log_message(f"ℹ️ Filtered down to {rows_after_filter} rows after applying tag mapping. Now pivoting the data.")

                if rows_after_filter == 0:
                    _log_message("⚠️ No data found with matching tags to insert. All fetched rows were ignored.")
                else:
                    # Pivot the data to a wide format
                    pivot_df = df.pivot_table(index="DateAndTime", columns="TAG", values="Val", aggfunc='first').reset_index()

                    # Step 5: Insert new data into PostgreSQL
                    _log_message(f"ℹ️ Inserting {len(pivot_df)} new row(s) into PostgreSQL.")
                    
                    # Prepare insert statement
                    cols = ','.join(f'"{col}"' for col in pivot_df.columns)
                    insert_query = f"INSERT INTO {config['PG_TABLE_NAME']} ({cols}) VALUES ({','.join(['%s'] * len(pivot_df.columns))})"
                    
                    # Create a list of tuples for executemany
                    data_to_insert = [tuple(row) for row in pivot_df.itertuples(index=False)]
                    
                    # Use executemany for efficiency
                    pg_cursor.executemany(insert_query, data_to_insert)
                    
                    pg_conn.commit()
                    _log_message(f"✅ Synced {len(pivot_df)} new row(s) from SQL Server to PostgreSQL.")
                    st.session_state.last_sync_time = pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')

        except pyodbc.Error as e:
            _log_message(f"❌ SQL Server connection failed. Error: {e}")
            st.error(f"❌ SQL Server connection failed. Check your server name, database, and network connectivity. Error: {e}")
            st.session_state.sync_running = False
        except psycopg2.OperationalError as e:
            _log_message(f"❌ PostgreSQL connection failed. Error: {e}")
            st.error(f"❌ PostgreSQL connection failed. Check your credentials and that the service is running. Error: {e}")
            st.session_state.sync_running = False
        except Exception as e:
            _log_message(f"❌ A general error occurred: {e}. Stopping sync.")
            st.error(f"❌ A general error occurred: {e}. Stopping sync.")
            st.session_state.sync_running = False
        finally:
            if sql_conn: sql_conn.close()
            if pg_conn: pg_conn.close()

        if st.session_state.sync_running:
            _log_message("💤 Waiting for 60 seconds before the next sync cycle...")
            time.sleep(60)

# ==============================================================================
# Main App Flow
# ==============================================================================
# --- Dependency Check ---
try:
    import pyodbc
    import psycopg2
except ImportError:
    st.error("❌ The required Python libraries ('pyodbc' or 'psycopg2') are not installed. Please run `pip install pyodbc psycopg2-binary` from your terminal.")
    st.stop()

# --- DB Setup ---
if submitted:
    if create_database_if_not_exists(host, port, user, password, db_name):
        create_pivoted_table_if_not_exists(host, port, user, password, db_name, pg_table_name)
    # Re-fetch config from the form for the main part of the app
    CONFIG['SQL_SERVER_NAME'] = sql_server
    CONFIG['SQL_DB_NAME'] = sql_db_name
    CONFIG['SQL_TABLE_NAME'] = sql_table_name
    CONFIG['PG_HOST'] = host
    CONFIG['PG_PORT'] = port
    CONFIG['PG_USER'] = user
    CONFIG['PG_DB_NAME'] = db_name
    CONFIG['PG_TABLE_NAME'] = pg_table_name
    st.session_state.pg_password = password
    

# --- Data Preview ---
st.header("🔍 SQL Server Data Preview")
st.info("ℹ️ This is a diagnostic preview to confirm the connection and data are correct.")
try:
    sql_conn = pyodbc.connect(f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={sql_server};DATABASE={sql_db_name};Trusted_Connection=yes;')
    sql_query = f"SELECT TOP 500 DateAndTime, TagIndex, Val FROM {sql_table_name} ORDER BY DateAndTime DESC;"
    preview_df = pd.read_sql(sql_query, sql_conn)
    st.dataframe(preview_df)
    sql_conn.close()
    st.info("✅ Preview successfully fetched. The data above shows the most recent 500 rows from your SQL Server.")
except pyodbc.Error as e:
    st.warning(f"⚠️ Could not connect to SQL Server for preview. Check your connection details in the code and your network. Error: {e}")
except Exception as e:
    st.warning(f"⚠️ An unexpected error occurred while fetching data for preview. Error: {e}")

# --- Live Data Diagnostic ---
st.header("🩺 Live Data Diagnostic")
st.info("ℹ️ This section shows the most recent timestamp in your SQL Server database to confirm it's updating.")
latest_sql_ts = get_latest_sql_timestamp(CONFIG['SQL_SERVER_NAME'], CONFIG['SQL_DB_NAME'], CONFIG['SQL_TABLE_NAME'])
if latest_sql_ts:
    st.info(f"✅ The latest timestamp found in the SQL Server table is: `{latest_sql_ts}`.")
else:
    st.warning("⚠️ Could not retrieve the latest timestamp from the SQL Server. The table might be empty or a connection error is occurring.")
st.markdown("---")

# --- Sync Controls & Log ---
st.header("⚙️ Sync Controls & Status")
col1, col2, col3 = st.columns(3)
if col1.button("🚀 Start Sync") and not st.session_state.sync_running:
    if 'pg_password' not in st.session_state:
        st.error("❌ Please enter your settings and click 'Save Settings' before starting the sync.")
    else:
        st.session_state.sync_running = True
        st.session_state.sync_log = [] # Clear the log
        st.session_state.sync_thread = threading.Thread(target=sync_continuously, args=(CONFIG, TAG_MAPPING, st.session_state.pg_password), daemon=True)
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

# Rerun the app to update the log
if st.session_state.sync_running:
    time.sleep(1)
    st.rerun()
