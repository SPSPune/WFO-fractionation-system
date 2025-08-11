import streamlit as st
import psycopg2
import os
import pandas as pd
import threading
import time
import sys
import pyodbc
import re
from psycopg2.errors import DuplicateDatabase, OperationalError as Psycopg2OperationalError
from pyodbc import OperationalError as PyodbcOperationalError

# ==============================================================================
#      _           _        _
# | |__  ___| |_ _  _ __ _| |_ ___| |__
# | '_ \/ _ \ __| | | |/ _` | __/ __| '_ \
# | |_) |  __/ |_| |_| | (_| | | |__ \ | | |
# |_.__/\___|\__|\__,_|\__|___/_| |_|
#
# Edit these values to configure your application
# ==============================================================================
# Use a default config, but the Streamlit form will override this
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
# This mapping is no longer used for the sync process itself, but is included
# for future use in the analysis portion of the Streamlit app.
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
if 'pg_password' not in st.session_state:
    st.session_state.pg_password = ""

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
    
    conn = None
    try:
        conn = psycopg2.connect(host=host, port=port, user=user, password=password, dbname="postgres")
        conn.autocommit = True
        cursor = conn.cursor()
        
        cursor.execute("SELECT 1 FROM pg_database WHERE datname = %s", (db_name,))
        if cursor.fetchone():
            st.info(f"ℹ️ PostgreSQL database `{db_name}` already exists. Skipping creation.")
            return True
            
        create_query = f"CREATE DATABASE \"{db_name}\""
        st.info(f"ℹ️ Attempting to create PostgreSQL database `{db_name}`...")
        cursor.execute(create_query)
        st.success(f"✅ PostgreSQL database `{db_name}` created successfully.")
        return True
    
    except Psycopg2OperationalError as e:
        st.error(f"❌ Cannot connect to PostgreSQL to create the database. Please check your **Host, Port, Username, and Password**. Error: {e}")
        return False
    except Exception as e:
        st.error(f"❌ An unexpected error occurred while creating the PostgreSQL database. Error: {e}")
        return False
    finally:
        if conn:
            conn.close()

def create_raw_table_if_not_exists(host, port, user, password, db_name, table_name):
    """
    Connects to the specific database and creates a raw data table
    that mirrors the SQL Server structure.
    """
    conn = None
    try:
        conn = psycopg2.connect(host=host, port=port, user=user, password=password, dbname=db_name)
        cursor = conn.cursor()
        create_query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            DateAndTime TIMESTAMP,
            TagIndex INT,
            Val FLOAT
        );
        """
        st.info(f"ℹ️ Attempting to create raw data table `{table_name}`...")
        cursor.execute(create_query)
        conn.commit()
        st.success(f"✅ Raw data table `{table_name}` verified/created successfully.")
        return True
    except Psycopg2OperationalError as e:
        st.error(f"❌ Could not connect to database `{db_name}` to create the table. Error: {e}")
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
    except PyodbcOperationalError as e:
        st.error(f"❌ Could not fetch latest timestamp from SQL Server. Check your server name and database permissions. Error: {e}")
        return None
    except Exception as e:
        st.error(f"❌ An unexpected error occurred while fetching the latest timestamp from SQL Server. Error: {e}")
        return None

def _log_message(message):
    """Thread-safe logging to a Streamlit session state variable."""
    st.session_state.sync_log.append(f"[{pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}] {message}")

# ---------------------------
# Sync Function (Diagnostic Mode) - REVISED
# ---------------------------
def sync_continuously_revised(config, password):
    """The main continuous sync loop, now with a direct insert approach."""
    _log_message("Starting continuous sync process...")
    
    while st.session_state.sync_running:
        sql_conn, pg_conn = None, None
        try:
            # Step 1: Connect to SQL Server
            conn_str = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={config["SQL_SERVER_NAME"]};DATABASE={config["SQL_DB_NAME"]};Trusted_Connection=yes;'
            sql_conn = pyodbc.connect(conn_str)
            sql_cursor = sql_conn.cursor()
            
            # Step 2: Get the latest timestamp from PostgreSQL's raw table
            _log_message("ℹ️ Connecting to PostgreSQL to get the latest timestamp...")
            pg_conn = psycopg2.connect(host=config['PG_HOST'], port=config['PG_PORT'], user=config['PG_USER'], password=password, dbname=config['PG_DB_NAME'])
            pg_cursor = pg_conn.cursor()
            pg_cursor.execute(f"SELECT MAX(DateAndTime) FROM {config['PG_TABLE_NAME']};")
            latest_timestamp = pg_cursor.fetchone()[0] or pd.Timestamp('1970-01-01')
            _log_message(f"ℹ️ Latest timestamp in PostgreSQL is: `{latest_timestamp}`.")

            # Step 3: Query SQL Server for new data
            _log_message(f"ℹ️ Fetching new data from SQL Server since `{latest_timestamp}`.")
            sql_query = f"""
            SELECT DateAndTime, TagIndex, Val
            FROM {config['SQL_TABLE_NAME']}
            WHERE DateAndTime > ?
            ORDER BY DateAndTime ASC;
            """
            sql_cursor.execute(sql_query, latest_timestamp)
            rows = sql_cursor.fetchall()
            
            _log_message(f"📁 Fetched {len(rows)} new rows from SQL Server.")

            if not rows:
                _log_message("📁 No new data found. Waiting for new data...")
            else:
                # Step 4: Insert new data directly into PostgreSQL
                _log_message(f"ℹ️ Inserting {len(rows)} new row(s) directly into PostgreSQL.")
                insert_query = f"INSERT INTO {config['PG_TABLE_NAME']} (DateAndTime, TagIndex, Val) VALUES (%s, %s, %s)"
                pg_cursor.executemany(insert_query, rows)
                pg_conn.commit()
                _log_message(f"✅ Synced {len(rows)} new row(s) from SQL Server to PostgreSQL.")
                st.session_state.last_sync_time = pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')

        except Exception as e:
            _log_message(f"❌ An error occurred: {e}. Stopping sync.")
            st.error(f"❌ An error occurred: {e}. Stopping sync.")
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
    st.session_state.pg_password = password
    # Call the new table creation function
    if create_database_if_not_exists(host, port, user, st.session_state.pg_password, db_name):
        create_raw_table_if_not_exists(host, port, user, st.session_state.pg_password, db_name, pg_table_name)
    # Re-fetch config from the form for the main part of the app
    CONFIG['SQL_SERVER_NAME'] = sql_server
    CONFIG['SQL_DB_NAME'] = sql_db_name
    CONFIG['SQL_TABLE_NAME'] = sql_table_name
    CONFIG['PG_HOST'] = host
    CONFIG['PG_PORT'] = port
    CONFIG['PG_USER'] = user
    CONFIG['PG_DB_NAME'] = db_name
    CONFIG['PG_TABLE_NAME'] = pg_table_name
    st.rerun()

# --- Data Preview ---
st.header("🔍 SQL Server Data Preview")
st.info("ℹ️ This is a diagnostic preview to confirm the connection and data are correct.")
try:
    sql_conn = pyodbc.connect(f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={CONFIG["SQL_SERVER_NAME"]};DATABASE={CONFIG["SQL_DB_NAME"]};Trusted_Connection=yes;')
    sql_query = f"SELECT TOP 500 DateAndTime, TagIndex, Val FROM {CONFIG['SQL_TABLE_NAME']} ORDER BY DateAndTime DESC;"
    preview_df = pd.read_sql(sql_query, sql_conn)
    st.dataframe(preview_df)
    sql_conn.close()
    st.info("✅ Preview successfully fetched. The data above shows the most recent 500 rows from your SQL Server.")
except (PyodbcOperationalError, Exception) as e:
    st.warning(f"⚠️ Could not connect to SQL Server for preview. Check your connection details in the code and your network. Error: {e}")

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
    if st.session_state.pg_password == "":
        st.error("❌ Please enter your PostgreSQL password in the sidebar and click 'Save Settings' before starting the sync.")
    else:
        st.session_state.sync_running = True
        st.session_state.sync_log = []
        st.session_state.sync_thread = threading.Thread(target=sync_continuously_revised, args=(CONFIG, st.session_state.pg_password), daemon=True)
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