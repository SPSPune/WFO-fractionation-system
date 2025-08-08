import streamlit as st
import psycopg2
import os
import pandas as pd
import threading
import time
import sys
import pyodbc
import re

# ---------------------------
# Global State
# ---------------------------
if 'sync_running' not in st.session_state:
    st.session_state.sync_running = False
if 'sync_thread' not in st.session_state:
    st.session_state.sync_thread = None

# ---------------------------
# Tag Mapping
# ---------------------------
tag_mapping = {
    251: "TI-31", 253: "TI-32", 254: "TI-33", 255: "TI-35", 256: "TI-35",
    257: "TI-36", 258: "TI-37", 259: "TI-38", 260: "TI-39", 261: "TI-40",
    270: "TI-41", 271: "TI-42", 273: "TI-43", 274: "TI-44", 275: "TI-45",
    272: "TI-42A", 279: "TI-54", 199: "TI-107", 201: "TI-109", 296: "TI-73A",
    297: "TI-73B", 280: "TI-55", 154: "PTT-03", 149: "PTB-03", 122: "LT-O5",
    123: "LT-06", 28: "FT-01", 46: "FT-07", 63: "FT-10", 37: "FT-04",
    225: "Tag-225", 226: "Tag-226", 227: "Tag-227", 228: "Tag-228",
    229: "Tag-229", 230: "Tag-230", 231: "Tag-231", 232: "Tag-232",
    233: "Tag-233", 234: "Tag-234", 235: "Tag-235"
}

# ---------------------------
# Page Config
# ---------------------------
st.set_page_config(page_title="SCADA SQL to PostgreSQL Sync", layout="centered")
st.title("🔄 SCADA SQL to PostgreSQL Sync Tool")

# ---------------------------
# Sidebar Input Form
# ---------------------------
st.sidebar.header("🔧 Sync Settings")
with st.sidebar.form("connection_form"):
    st.subheader("SQL Server Details")
    sql_server = st.text_input("SQL Server Name", value=r"DESKTOP-DG1Q26L\SQLEXPRESS")
    sql_db_name = st.text_input("SQL Server Database Name", help="The name of the database that contains your SCADA data.")
    sql_table_name = st.text_input("SQL Server Data Table Name", help="The name of the table that contains your SCADA data.", value="dbo.FloaTable")

    st.subheader("PostgreSQL Details")
    host = st.text_input("Host", value="localhost")
    port = st.text_input("Port", value="5432")
    user = st.text_input("Username", value="postgres")
    password = st.text_input("Password", type="password")
    db_name = st.text_input("PostgreSQL Database Name", help="Use a simple name with no spaces or special characters.")
    pg_table_name = st.text_input("Target Table Name", value="scada_data")

    submitted = st.form_submit_button("✅ Save Settings")

# ---------------------------
# DB Setup
# ---------------------------
def is_valid_db_name(name):
    """Checks if a string is a valid PostgreSQL database name."""
    if not name:
        return False
    return bool(re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', name))

def create_database_if_not_exists(host, port, user, password, db_name):
    """Tries to connect to the default 'postgres' database to create a new one."""
    if not is_valid_db_name(db_name):
        st.error(f"❌ Invalid PostgreSQL database name: `{db_name}`. Please use a simple name with only letters, numbers, and underscores (e.g., `scada_db`).")
        return False

    try:
        conn = psycopg2.connect(host=host, port=port, user=user, password=password, dbname="postgres")
        conn.autocommit = True
        cursor = conn.cursor()
        
        create_query = f"CREATE DATABASE \"{db_name}\""
        
        cursor.execute(f"SELECT 1 FROM pg_database WHERE datname = '{db_name}'")
        if not cursor.fetchone():
            cursor.execute(create_query)
            st.success(f"✅ PostgreSQL database `{db_name}` created successfully.")
        else:
            st.info(f"ℹ️ PostgreSQL database `{db_name}` already exists.")
        cursor.close()
        conn.close()
    except psycopg2.OperationalError as e:
        st.error(f"❌ Cannot connect to PostgreSQL to create the database. Please check your **Host, Port, Username, and Password**. Error: {e}")
        return False
    except Exception as e:
        st.error(f"❌ An unexpected error occurred while creating the PostgreSQL database. Error: {e}")
        return False
    return True

def create_pivoted_table_if_not_exists(conn, table_name, tag_mapping):
    """Creates the target table in PostgreSQL if it doesn't exist."""
    cursor = conn.cursor()
    columns = ",\n".join([f'"{tag}" FLOAT' for tag in tag_mapping.values()])
    create_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        DateAndTime TIMESTAMP,
        {columns}
    );
    """
    try:
        cursor.execute(create_query)
        conn.commit()
        st.success(f"✅ Table `{table_name}` verified/created successfully.")
    except Exception as e:
        st.error(f"❌ Error creating the table `{table_name}`. Please check if your **table name is valid** and if the database user has **permissions**. Error: {e}")
        return False
    finally:
        cursor.close()
    return True

# ---------------------------
# Sync Function
# ---------------------------
def sync_continuously(sql_server, sql_db_name, sql_table_name, host, port, user, password, db_name, pg_table_name, tag_mapping):
    """The main continuous sync loop, now connecting directly to SQL Server."""
    if not sql_table_name:
        st.error("❌ Please enter the SQL Server table name in the sidebar.")
        st.session_state.sync_running = False
        return
        
    while st.session_state.sync_running:
        try:
            # 1. Connect to SQL Server
            # The SERVER value now uses r"..." to handle backslashes correctly
            conn_str = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={sql_server};DATABASE={sql_db_name};Trusted_Connection=yes;'
            sql_conn = pyodbc.connect(conn_str)
            sql_cursor = sql_conn.cursor()

            # 2. Check if the table exists in SQL Server before querying it
            table_check_query = f"""
            SELECT 1 FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_SCHEMA = 'dbo' 
            AND TABLE_NAME = ?;
            """
            sql_cursor.execute(table_check_query, sql_table_name)
            if not sql_cursor.fetchone():
                st.error(f"❌ SQL Server table '{sql_table_name}' not found in database '{sql_db_name}'. Please verify the table name.")
                st.session_state.sync_running = False
                sql_cursor.close()
                sql_conn.close()
                continue
            
            # 3. Get the latest timestamp from PostgreSQL to avoid duplicates
            pg_conn = psycopg2.connect(host=host, port=port, user=user, password=password, dbname=db_name)
            pg_cursor = pg_conn.cursor()
            
            pg_cursor.execute(f"SELECT MAX(DateAndTime) FROM {pg_table_name};")
            latest_timestamp = pg_cursor.fetchone()[0] or '1970-01-01'

            # 4. Query SQL Server for new data (newer than the latest timestamp)
            sql_query = f"""
            SELECT DateAndTime, TagIndex, Val
            FROM [{sql_table_name}] 
            WHERE DateAndTime > ?
            ORDER BY DateAndTime ASC;
            """
            sql_cursor.execute(sql_query, latest_timestamp)
            rows = sql_cursor.fetchall()

            if not rows:
                st.info("📁 No new data found in SQL Server. Waiting for new data...")
            else:
                # 5. Process and Pivot the new data
                df = pd.DataFrame(rows, columns=["DateAndTime", "TagIndex", "Val"])
                df["TAG"] = df["TagIndex"].map(tag_mapping)
                df.dropna(subset=["TAG"], inplace=True)
                pivot_df = df.pivot_table(index="DateAndTime", columns="TAG", values="Val", aggfunc='first').reset_index()

                if pivot_df.empty:
                    st.warning("⚠️ No valid data found to insert.")
                else:
                    # 6. Insert new data into PostgreSQL
                    for _, row in pivot_df.iterrows():
                        cols = ','.join(f'"{col}"' for col in row.index)
                        vals = []
                        for val in row.values:
                            if pd.isna(val):
                                vals.append('NULL')
                            elif isinstance(val, (int, float)):
                                vals.append(str(val))
                            else:
                                vals.append(f"'{val}'")
                        vals_str = ','.join(vals)
                        insert = f'INSERT INTO {pg_table_name} ({cols}) VALUES ({vals_str});'
                        pg_cursor.execute(insert)
                    
                    pg_conn.commit()
                    st.success(f"✅ Synced {len(pivot_df)} new row(s) from SQL Server to PostgreSQL.")
            
            sql_cursor.close()
            sql_conn.close()
            pg_cursor.close()
            pg_conn.close()

        except pyodbc.Error as e:
            st.error(f"❌ SQL Server connection failed. Please check your server name and database name. Error: {e}")
            st.session_state.sync_running = False
        except psycopg2.OperationalError as e:
            st.error(f"❌ PostgreSQL connection failed. Please check your **credentials and that the service is running**. Error: {e}")
            st.session_state.sync_running = False
        except Exception as e:
            st.error(f"❌ General sync error: {e}")
            st.session_state.sync_running = False
        
        time.sleep(60)  # Wait 1 minute

# ---------------------------
# Main App Logic
# ---------------------------
if submitted:
    try:
        import pyodbc
    except ImportError:
        st.error("❌ The 'pyodbc' library is not installed. Please run `pip install pyodbc` from your terminal.")
    
    if create_database_if_not_exists(host, port, user, password, db_name):
        try:
            conn = psycopg2.connect(host=host, port=port, user=user, password=password, dbname=db_name)
            create_pivoted_table_if_not_exists(conn, pg_table_name, tag_mapping)
            conn.close()
        except psycopg2.OperationalError as e:
            st.error(f"❌ Could not connect to the new database `{db_name}` to create the table. Please verify the database exists and your credentials are correct. Error: {e}")
        except Exception as e:
            st.error(f"❌ An error occurred during table creation. Error: {e}")

if st.button("🚀 Start Sync") and not st.session_state.sync_running and submitted:
    if not sql_table_name:
        st.error("❌ Please enter the SQL Server table name before starting the sync.")
    else:
        st.session_state.sync_running = True
        st.session_state.sync_thread = threading.Thread(target=sync_continuously, args=(sql_server, sql_db_name, sql_table_name, host, port, user, password, db_name, pg_table_name, tag_mapping))
        st.session_state.sync_thread.start()
        st.info("⏳ Sync started...")

if st.button("🛑 Stop Sync") and st.session_state.sync_running:
    st.session_state.sync_running = False
    st.warning("🛑 Sync stopped.")
