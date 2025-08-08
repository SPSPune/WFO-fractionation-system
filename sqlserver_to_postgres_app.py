import streamlit as st
import psycopg2
import os
import glob
import pandas as pd
import threading
import time

# ---------------------------
# Global State
# ---------------------------
if 'sync_running' not in st.session_state:
    st.session_state.sync_running = False
if 'processed_files' not in st.session_state:
    st.session_state.processed_files = set()
if 'sync_thread' not in st.session_state:
    st.session_state.sync_thread = None

# ---------------------------
# Tag Mapping (from image)
# ---------------------------
# Expanded tag mapping to include values from the user's screenshot
tag_mapping = {
    251: "TI-31", 253: "TI-32", 254: "TI-33", 255: "TI-34", 256: "TI-35",
    257: "TI-36", 258: "TI-37", 259: "TI-38", 260: "TI-39", 261: "TI-40",
    270: "TI-41", 271: "TI-42", 273: "TI-43", 274: "TI-44", 275: "TI-45",
    272: "TI-42A", 279: "TI-54", 199: "TI-107", 201: "TI-109", 296: "TI-73A",
    297: "TI-73B", 280: "TI-55", 154: "PTT-03", 149: "PTB-03", 122: "LT-O5",
    123: "LT-06", 28: "FT-01", 46: "FT-07", 63: "FT-10", 37: "FT-04",
    # Added new tags from the user's screenshot
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
    st.subheader("PostgreSQL Details")
    host = st.text_input("Host", value="localhost")
    port = st.text_input("Port", value="5432")
    user = st.text_input("Username", value="postgres")
    password = st.text_input("Password", type="password")
    db_name = st.text_input("Database Name")
    table_name = st.text_input("Target Table Name", value="scada_data")

    st.subheader("SQL File Folder")
    folder_path = st.text_input("Enter the folder path where .sql files are stored", placeholder="e.g., G:/Monika/WFO Fractionation System/sql_files")

    submitted = st.form_submit_button("✅ Save Settings")

# ---------------------------
# DB Setup
# ---------------------------
def create_database_if_not_exists(host, port, user, password, db_name):
    """Tries to connect to the default 'postgres' database to create a new one."""
    try:
        conn = psycopg2.connect(host=host, port=port, user=user, password=password, dbname="postgres")
        conn.autocommit = True
        cursor = conn.cursor()
        cursor.execute(f"SELECT 1 FROM pg_database WHERE datname = '{db_name}'")
        if not cursor.fetchone():
            cursor.execute(f"CREATE DATABASE {db_name}")
            st.success(f"✅ Database `{db_name}` created successfully.")
        else:
            st.info(f"ℹ️ Database `{db_name}` already exists.")
        cursor.close()
        conn.close()
    except psycopg2.OperationalError as e:
        st.error(f"❌ Cannot connect to PostgreSQL to create the database. Please check your **Host, Port, Username, and Password**. Error: {e}")
        return False
    except Exception as e:
        st.error(f"❌ An unexpected error occurred while creating the database. Error: {e}")
        return False
    return True

def create_pivoted_table_if_not_exists(conn, table_name, tag_mapping):
    """Creates the target table if it doesn't exist."""
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
def sync_continuously(host, port, user, password, db_name, table_name, folder_path, tag_mapping):
    """The main continuous sync loop."""
    while st.session_state.sync_running:
        try:
            # Check if the folder path is valid before trying to connect
            if not os.path.isdir(folder_path):
                st.error(f"❌ Cannot reach the path: `{folder_path}`. Please verify the folder exists.")
                st.session_state.sync_running = False
                continue

            conn = psycopg2.connect(host=host, port=port, user=user, password=password, dbname=db_name)
            conn.autocommit = True
            cursor = conn.cursor()

            sql_files = glob.glob(os.path.join(folder_path, "*.sql"))
            new_files = [f for f in sql_files if f not in st.session_state.processed_files]
            
            if not new_files:
                st.info("📁 No new files yet. Waiting for new .sql files...")
            else:
                all_data = []
                for file in new_files:
                    try:
                        with open(file, 'r') as f:
                            raw_sql = f.read()
                        
                        cursor.execute("DROP TABLE IF EXISTS temp_raw;")
                        cursor.execute("""
                            CREATE TEMP TABLE temp_raw (
                                DateAndTime TIMESTAMP,
                                TagIndex INT,
                                Val FLOAT
                            );
                        """)
                        cursor.execute(raw_sql)
                        
                        cursor.execute("SELECT * FROM temp_raw;")
                        rows = cursor.fetchall()
                        
                        df = pd.DataFrame(rows, columns=["DateAndTime", "TagIndex", "Val"])
                        df["TAG"] = df["TagIndex"].map(tag_mapping)
                        
                        df.dropna(subset=["TAG"], inplace=True)
                        
                        pivot_df = df.pivot_table(index="DateAndTime", columns="TAG", values="Val", aggfunc='first').reset_index()
                        all_data.append(pivot_df)
                        
                        st.session_state.processed_files.add(file)
                        st.success(f"✅ Processed file: {os.path.basename(file)}")

                    except FileNotFoundError:
                        st.error(f"❌ File not found: `{os.path.basename(file)}`. This file may have been moved or deleted.")
                        continue
                    except Exception as e:
                        st.error(f"❌ Error reading or processing file `{os.path.basename(file)}`. The file might be corrupted or in an incorrect format. Error: {e}")
                        continue

                if all_data:
                    combined = pd.concat(all_data, ignore_index=True).sort_values("DateAndTime")
                    
                    if combined.empty:
                        st.warning("⚠️ No valid data found in the new files to insert.")
                        
                    # Constructing the insert query more robustly
                    for _, row in combined.iterrows():
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
                        insert = f'INSERT INTO {table_name} ({cols}) VALUES ({vals_str});'
                        cursor.execute(insert)
                    
                    conn.commit()
                    st.success(f"✅ Synced {len(new_files)} new file(s) to the table.")
            
            cursor.close()
            conn.close()

        except psycopg2.OperationalError as e:
            st.error(f"❌ Database connection failed. Please check your **credentials and that the PostgreSQL service is running**. Error: {e}")
            st.session_state.sync_running = False
        except Exception as e:
            st.error(f"❌ General sync error: {e}")
            st.session_state.sync_running = False
        
        time.sleep(60)  # Wait 1 minute

# ---------------------------
# Main App Logic
# ---------------------------
if submitted:
    if create_database_if_not_exists(host, port, user, password, db_name):
        try:
            conn = psycopg2.connect(host=host, port=port, user=user, password=password, dbname=db_name)
            create_pivoted_table_if_not_exists(conn, table_name, tag_mapping)
            conn.close()
        except psycopg2.OperationalError as e:
            st.error(f"❌ Could not connect to the new database `{db_name}` to create the table. Please verify the database exists and your credentials are correct. Error: {e}")
        except Exception as e:
            st.error(f"❌ An error occurred during table creation. Error: {e}")

if st.button("🚀 Start Sync") and not st.session_state.sync_running and submitted:
    st.session_state.sync_running = True
    st.session_state.sync_thread = threading.Thread(target=sync_continuously, args=(host, port, user, password, db_name, table_name, folder_path, tag_mapping))
    st.session_state.sync_thread.start()
    st.info("⏳ Sync started...")

if st.button("🛑 Stop Sync") and st.session_state.sync_running:
    st.session_state.sync_running = False
    st.warning("🛑 Sync stopped.")
