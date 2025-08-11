import streamlit as st
import os
import pandas as pd
import threading
import time
import sys
import pyodbc
import re
from pyodbc import OperationalError as PyodbcOperationalError

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
    "SQL_SERVER_NAME": r"DESKTOP-DG1Q26L\SQLEXPRESS",
    "SQL_DB_NAME": "JSCPL",
    "SQL_TABLE_NAME_RAW": "dbo.FloatTable",
    "SQL_TABLE_NAME_PIVOTED": "dbo.PivotedData"  # New target table name for SQL Server
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
    st.subheader("SQL Server Details")
    sql_server = st.text_input("SQL Server Name", value=CONFIG["SQL_SERVER_NAME"], help="The server where your SCADA data is located.")
    sql_db_name = st.text_input("SQL Server Database Name", value=CONFIG["SQL_DB_NAME"], help="The name of the database that contains your SCADA data.")
    sql_table_name_raw = st.text_input("Source Raw Table Name", value=CONFIG["SQL_TABLE_NAME_RAW"], help="The name of the raw data table.")
    sql_table_name_pivoted = st.text_input("Target Pivoted Table Name", value=CONFIG["SQL_TABLE_NAME_PIVOTED"], help="The name of the table to store the pivoted data.")

    submitted = st.form_submit_button("✅ Save Settings and Initialize")

# ---------------------------
# DB Utility Functions
# ---------------------------

def create_sql_pivoted_table_if_not_exists(sql_server, sql_db_name, table_name):
    """
    Connects to the SQL Server database and creates a pivoted data table
    with a column for each tag.
    """
    conn = None
    try:
        conn_str = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={sql_server};DATABASE={sql_db_name};Trusted_Connection=yes;'
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        
        # Dynamically build the columns based on the TAG_MAPPING
        columns = ", ".join([f'"{tag_name}" FLOAT' for tag_name in TAG_MAPPING.values()])
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
        st.error(f"❌ Could not connect to SQL Server to create the pivoted table. Check your connection details and permissions. Error: {e}")
        return False
    except Exception as e:
        st.error(f"❌ An error occurred during pivoted table creation. Error: {e}")
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
        # Handle case where table is empty
        if df.iloc[0, 0] is None:
            return pd.Timestamp('1970-01-01')
        return df.iloc[0, 0]
    except PyodbcOperationalError as e:
        st.error(f"❌ Could not fetch latest timestamp from SQL Server. Check your server name and database permissions. Error: {e}")
        return None
    except Exception as e:
        st.error(f"❌ An unexpected error occurred while fetching the latest timestamp from SQL Server. Error: {e}")
        return None
    finally:
        if conn:
            conn.close()

def _log_message(message):
    """Thread-safe logging to a Streamlit session state variable."""
    st.session_state.sync_log.append(f"[{pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}] {message}")

# ---------------------------
# Sync Function (Corrected with SQL-to-SQL Pivoting)
# ---------------------------
def sync_continuously(config):
    """The main continuous sync loop, now with SQL-to-SQL pivoting logic."""
    _log_message("Starting continuous sync process with SQL-to-SQL pivoting...")
    
    while st.session_state.sync_running:
        sql_conn_read, sql_conn_write = None, None
        try:
            # Step 1: Get the latest timestamp from the pivoted SQL Server table
            latest_timestamp = get_latest_sql_timestamp(
                config['SQL_SERVER_NAME'], config['SQL_DB_NAME'], config['SQL_TABLE_NAME_PIVOTED']
            ) or pd.Timestamp('1970-01-01')
            
            _log_message(f"ℹ️ Latest pivoted timestamp in SQL Server is: `{latest_timestamp}`.")

            # Step 2: Connect to SQL Server and fetch new raw data
            _log_message(f"ℹ️ Fetching new raw data from SQL Server since `{latest_timestamp}`.")
            conn_str = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={config["SQL_SERVER_NAME"]};DATABASE={config["SQL_DB_NAME"]};Trusted_Connection=yes;'
            sql_conn_read = pyodbc.connect(conn_str)
            sql_query = f"""
            SELECT DateAndTime, TagIndex, Val
            FROM {config['SQL_TABLE_NAME_RAW']}
            WHERE DateAndTime > ?
            ORDER BY DateAndTime ASC;
            """
            
            raw_df = pd.read_sql(sql_query, sql_conn_read, params=[latest_timestamp])
            sql_conn_read.close()
            
            _log_message(f"📁 Fetched {len(raw_df)} new raw rows from SQL Server.")

            if raw_df.empty:
                _log_message("📁 No new data found. Waiting for new data...")
            else:
                # Step 3: Pivot the data using pandas
                raw_df['DateAndTime'] = pd.to_datetime(raw_df['DateAndTime']).dt.floor('T')
                raw_df = raw_df.sort_values(by='DateAndTime')
                
                raw_df.drop_duplicates(subset=['DateAndTime', 'TagIndex'], keep='first', inplace=True)
                
                raw_df['TagIndex'] = raw_df['TagIndex'].map(TAG_MAPPING)

                pivoted_df = raw_df.pivot_table(
                    index='DateAndTime',
                    columns='TagIndex',
                    values='Val',
                    aggfunc='first'
                ).reset_index()

                _log_message(f"🔄 Pivoted data for timestamps up to: `{pivoted_df['DateAndTime'].max()}`.")

                tag_columns = list(TAG_MAPPING.values())
                missing_cols = set(tag_columns) - set(pivoted_df.columns)
                for col in missing_cols:
                    pivoted_df[col] = None

                pivoted_df = pivoted_df[['DateAndTime'] + tag_columns]

                # Step 4: Insert the pivoted data into a new SQL Server table
                _log_message(f"ℹ️ Inserting {len(pivoted_df)} new pivoted rows into SQL Server.")
                sql_conn_write = pyodbc.connect(conn_str)
                cursor = sql_conn_write.cursor()

                # Build the INSERT statement dynamically
                columns = ", ".join([f'[{col}]' for col in pivoted_df.columns])
                placeholders = ", ".join(['?'] * len(pivoted_df.columns))
                insert_query = f"INSERT INTO {config['SQL_TABLE_NAME_PIVOTED']} ({columns}) VALUES ({placeholders});"

                # Convert DataFrame to a list of tuples for executemany
                rows_to_insert = [tuple(row) for row in pivoted_df.itertuples(index=False, name=None)]
                
                # Check for existing timestamps to avoid key violation errors
                existing_timestamps = []
                if len(rows_to_insert) > 0:
                    min_ts = pivoted_df['DateAndTime'].min()
                    max_ts = pivoted_df['DateAndTime'].max()
                    check_query = f"SELECT DateAndTime FROM {config['SQL_TABLE_NAME_PIVOTED']} WHERE DateAndTime BETWEEN ? AND ?;"
                    cursor.execute(check_query, (min_ts, max_ts))
                    existing_timestamps = {row[0] for row in cursor.fetchall()}
                
                # Filter out rows with existing timestamps
                new_rows_to_insert = [row for row in rows_to_insert if row[0] not in existing_timestamps]

                if new_rows_to_insert:
                    cursor.executemany(insert_query, new_rows_to_insert)
                    sql_conn_write.commit()
                    _log_message(f"✅ Synced {len(new_rows_to_insert)} new pivoted row(s) to SQL Server.")
                else:
                    _log_message("ℹ️ No new pivoted rows to insert after checking for existing data.")

                st.session_state.last_sync_time = pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')

        except Exception as e:
            _log_message(f"❌ A general error occurred: {e}. Stopping sync.")
            st.error(f"❌ A general error occurred: {e}. Stopping sync.")
            st.session_state.sync_running = False
        finally:
            if sql_conn_read: sql_conn_read.close()
            if sql_conn_write: sql_conn_write.close()

        if st.session_state.sync_running:
            _log_message("💤 Waiting for 60 seconds before the next sync cycle...")
            time.sleep(60)

# ==============================================================================
# Main App Flow
# ==============================================================================
# --- Dependency Check ---
try:
    import pyodbc
    import pandas as pd
except ImportError:
    st.error("❌ The required Python libraries ('pyodbc' or 'pandas') are not installed. Please run `pip install pyodbc pandas` from your terminal.")
    st.stop()

# --- DB Setup ---
if submitted:
    create_sql_pivoted_table_if_not_exists(sql_server, sql_db_name, sql_table_name_pivoted)
    CONFIG['SQL_SERVER_NAME'] = sql_server
    CONFIG['SQL_DB_NAME'] = sql_db_name
    CONFIG['SQL_TABLE_NAME_RAW'] = sql_table_name_raw
    CONFIG['SQL_TABLE_NAME_PIVOTED'] = sql_table_name_pivoted
    st.rerun()

# --- Live Data Diagnostic ---
st.header("🩺 Live Data Diagnostic")
st.info("ℹ️ This section shows the most recent timestamp in your raw and pivoted SQL Server databases to confirm the sync is working.")
latest_sql_raw_ts = get_latest_sql_timestamp(CONFIG['SQL_SERVER_NAME'], CONFIG['SQL_DB_NAME'], CONFIG['SQL_TABLE_NAME_RAW'])
latest_sql_pivoted_ts = get_latest_sql_timestamp(CONFIG['SQL_SERVER_NAME'], CONFIG['SQL_DB_NAME'], CONFIG['SQL_TABLE_NAME_PIVOTED'])
if latest_sql_raw_ts:
    st.info(f"✅ The latest timestamp found in the **raw** SQL Server table is: `{latest_sql_raw_ts}`.")
else:
    st.warning("⚠️ Could not retrieve the latest timestamp from the raw SQL Server table. Check your connection settings.")
st.markdown("---")
if latest_sql_pivoted_ts:
    st.info(f"✅ The latest timestamp found in the **pivoted** SQL Server table is: `{latest_sql_pivoted_ts}`.")
else:
    st.warning("⚠️ The pivoted SQL Server table may be empty or not yet created. The sync process will create it.")

# --- Sync Controls & Log ---
st.header("⚙️ Sync Controls & Status")
col1, col2, col3 = st.columns(3)
if col1.button("🚀 Start Sync") and not st.session_state.sync_running:
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
