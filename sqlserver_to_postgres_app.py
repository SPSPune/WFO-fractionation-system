# Import necessary libraries
import streamlit as st
import pandas as pd
import threading
import time
import json
import pyodbc
import psycopg2
from psycopg2 import sql

# --- 1. CONFIGURATION ---
# Load configuration from a separate file for easier management.
try:
    with open('config.json', 'r') as f:
        CONFIG = json.load(f)
except FileNotFoundError:
    st.error("❌ Error: 'config.json' not found. Please create this file with your database configurations.")
    st.stop()
except json.JSONDecodeError:
    st.error("❌ Error: 'config.json' is not a valid JSON file. Please check its syntax.")
    st.stop()

# --- 2. GLOBAL STATE AND UI SETUP ---
# Use session state to manage the sync running status and logging messages
if 'sync_running' not in st.session_state:
    st.session_state.sync_running = False

if 'log_messages' not in st.session_state:
    st.session_state.log_messages = []

# Function to add messages to the log and update the UI
def _log_message(message):
    st.session_state.log_messages.append(f"{time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime())} - {message}")
    # Keep the log to a reasonable length
    if len(st.session_state.log_messages) > 100:
        st.session_state.log_messages.pop(0)

# Main UI title and status display
st.title("Data Synchronization Manager")
st.subheader("SQL Server to PostgreSQL")

# Display a status badge
if st.session_state.sync_running:
    st.success("🟢 Sync Status: Running")
else:
    st.warning("🔴 Sync Status: Stopped")

# Control buttons
col1, col2 = st.columns(2)
with col1:
    if st.button("Start Sync", use_container_width=True, type="primary", disabled=st.session_state.sync_running):
        st.session_state.sync_running = True
        st.session_state.log_messages = [] # Clear logs on start
        _log_message("✅ Sync process started.")
        # Start the sync in a separate thread to prevent blocking the UI
        threading.Thread(target=sync_continuously, daemon=True).start()
        st.experimental_rerun()
with col2:
    if st.button("Stop Sync", use_container_width=True, type="secondary", disabled=not st.session_state.sync_running):
        st.session_state.sync_running = False
        _log_message("🛑 Sync process stopped by user.")
        st.experimental_rerun()

# Log display area
st.markdown("---")
st.markdown("### Live Log")
log_area = st.empty()
with log_area:
    st.text_area("Log Output", "\n".join(st.session_state.log_messages), height=300, key="log_output")

# --- 3. SYNCHRONIZATION LOGIC ---
def sync_continuously():
    """
    This function runs in a background thread to continuously sync data.
    """
    while st.session_state.sync_running:
        _log_message("ℹ️ Starting a new sync cycle...")

        try:
            # Step 1: Connect to SQL Server
            sql_conn = pyodbc.connect(
                f'DRIVER={{ODBC Driver 17 for SQL Server}};'
                f'SERVER={CONFIG["SQL_SERVER_NAME"]};'
                f'DATABASE={CONFIG["SQL_DB_NAME"]};'
                f'Trusted_Connection=yes;'
            )
            _log_message("✅ Connected to SQL Server.")

            # Step 2: Query data from SQL Server
            sql_query = f"SELECT [TAG], [Val], [DateAndTime] FROM {CONFIG['SQL_TABLE_NAME']}"
            df = pd.read_sql(sql_query, sql_conn)
            sql_conn.close()
            _log_message(f"✅ Fetched {len(df)} rows from SQL Server.")

            # Step 3: Prepare data for PostgreSQL
            # Pivot the data to a wide format to match the PostgreSQL table schema
            pivot_df = df.pivot_table(index="DateAndTime", columns="TAG", values="Val", aggfunc='first').reset_index()
            
            # Remove any columns that are all NaN (optional, but good practice)
            pivot_df.dropna(axis=1, how='all', inplace=True)
            pivot_df.dropna(inplace=True)

            # --- CRITICAL DEBUGGING SECTION ---
            # This section helps you verify the DataFrame before insertion.
            _log_message(f"ℹ️ Prepared {len(pivot_df)} rows for insertion. Previewing the data...")
            _log_message(f"DataFrame columns: {list(pivot_df.columns)}")
            _log_message(f"DataFrame dtypes:\n{pivot_df.dtypes.to_string()}")
            _log_message(f"First 5 rows of data:\n{pivot_df.head().to_string()}")
            # --- END OF CRITICAL DEBUGGING SECTION ---

            # Step 4: Connect to PostgreSQL
            pg_conn = psycopg2.connect(
                host=CONFIG["PG_HOST"],
                port=CONFIG["PG_PORT"],
                user=CONFIG["PG_USER"],
                dbname=CONFIG["PG_DB_NAME"]
            )
            pg_cursor = pg_conn.cursor()
            _log_message("✅ Connected to PostgreSQL.")

            # Step 5: Insert data into PostgreSQL
            if not pivot_df.empty:
                # Define the target table and columns
                table_name = CONFIG['PG_TABLE_NAME']
                columns = [sql.Identifier(col) for col in pivot_df.columns]
                # Prepare the SQL insert statement
                insert_query = sql.SQL("INSERT INTO {} ({}) VALUES ({}) ON CONFLICT ([your_conflict_column_here]) DO NOTHING").format(
                    sql.Identifier(table_name),
                    sql.SQL(', ').join(columns),
                    sql.SQL(', ').join(sql.Placeholder() * len(columns))
                )
                
                # Convert the DataFrame to a list of tuples for insertion
                data_to_insert = [tuple(row) for row in pivot_df.to_numpy()]
                
                pg_cursor.executemany(insert_query, data_to_insert)
                pg_conn.commit()
                _log_message(f"✅ Successfully inserted {len(data_to_insert)} new rows into PostgreSQL.")

            pg_cursor.close()
            pg_conn.close()
            
        except pyodbc.Error as e:
            _log_message(f"❌ SQL Server connection/query failed. Error: {e}")
            st.session_state.sync_running = False
        except psycopg2.OperationalError as e:
            _log_message(f"❌ PostgreSQL connection failed. Error: {e}")
            st.session_state.sync_running = False
        except psycopg2.Error as e: # <-- Catch specific insertion errors here
            _log_message(f"❌ PostgreSQL insertion error: {e}")
            st.error(f"❌ A data insertion error occurred. Please check the log for details. Error: {e}")
            st.session_state.sync_running = False
        except Exception as e:
            _log_message(f"❌ A general error occurred: {e}. Stopping sync.")
            st.session_state.sync_running = False

        # Pause for a specified interval before the next sync
        if st.session_state.sync_running:
            _log_message("ℹ️ Sync cycle complete. Waiting for 60 seconds...")
            time.sleep(60)

    # Final log message after the loop ends
    _log_message("🛑 Sync thread has stopped.")
    st.experimental_rerun()

# --- 4. STARTING THE APP ---
if __name__ == "__main__":
    if st.session_state.sync_running:
        _log_message("✅ Sync process is already running.")
