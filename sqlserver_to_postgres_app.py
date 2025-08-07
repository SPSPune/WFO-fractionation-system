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
sync_thread = None
sync_running = False
processed_files = set()

# ---------------------------
# Tag Mapping (from image)
# ---------------------------
tag_mapping = {
    251: "TI-31", 253: "TI-32", 254: "TI-33", 255: "TI-34", 256: "TI-35",
    257: "TI-36", 258: "TI-37", 259: "TI-38", 260: "TI-39", 261: "TI-40",
    270: "TI-41", 271: "TI-42", 273: "TI-43", 274: "TI-44", 275: "TI-45",
    272: "TI-42A", 279: "TI-54", 199: "TI-107", 201: "TI-109", 296: "TI-73A",
    297: "TI-73B", 280: "TI-55", 154: "PTT-03", 149: "PTB-03", 122: "LT-O5",
    123: "LT-06", 28: "FT-01", 46: "FT-07", 63: "FT-10", 37: "FT-04"
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
# Placeholders
# ---------------------------
status_placeholder = st.empty()
error_placeholder = st.empty()

# ---------------------------
# DB Setup
# ---------------------------
def create_database_if_not_exists():
    try:
        conn = psycopg2.connect(host=host, port=port, user=user, password=password, dbname="postgres")
        conn.autocommit = True
        cursor = conn.cursor()
        cursor.execute(f"SELECT 1 FROM pg_database WHERE datname = '{db_name}'")
        if not cursor.fetchone():
            cursor.execute(f"CREATE DATABASE {db_name}")
            status_placeholder.success(f"✅ Database `{db_name}` created.")
        else:
            status_placeholder.info(f"ℹ️ Database `{db_name}` already exists.")
        cursor.close()
        conn.close()
    except Exception as e:
        error_placeholder.error(f"❌ Error creating DB: {e}")

def create_pivoted_table_if_not_exists(conn):
    cursor = conn.cursor()
    columns = ",\n".join([f'"{tag}" FLOAT' for tag in tag_mapping.values()])
    create_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        DateAndTime TIMESTAMP,
        {columns}
    );
    """
    cursor.execute(create_query)
    cursor.close()

# ---------------------------
# Sync Function
# ---------------------------
def sync_continuously():
    global sync_running
    while sync_running:
        try:
            conn = psycopg2.connect(host=host, port=port, user=user, password=password, dbname=db_name)
            create_pivoted_table_if_not_exists(conn)
            cursor = conn.cursor()

            sql_files = glob.glob(os.path.join(folder_path, "*.sql"))
            new_files = [f for f in sql_files if f not in processed_files]
            if not new_files:
                status_placeholder.info("📁 No new files yet.")
            else:
                all_data = []
                for file in new_files:
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
                        processed_files.add(file)

                if all_data:
                    combined = pd.concat(all_data, ignore_index=True).sort_values("DateAndTime")
                    for _, row in combined.iterrows():
                        cols = ','.join(f'"{col}"' for col in row.index)
                        vals = ','.join("NULL" if pd.isna(val) else f"{val}" if isinstance(val, (int, float)) else f"'{val}'" for val in row.values)
                        insert = f'INSERT INTO {table_name} ({cols}) VALUES ({vals});'
                        cursor.execute(insert)
                    conn.commit()
                    status_placeholder.success(f"✅ Synced {len(new_files)} new file(s).")
            cursor.close()
            conn.close()
        except Exception as e:
            error_placeholder.error(f"❌ Sync error: {e}")
        time.sleep(60)  # Wait 1 minute

# ---------------------------
# Sync Button Controls
# ---------------------------
if submitted:
    create_database_if_not_exists()

if st.button("🚀 Start Sync") and not sync_running:
    sync_running = True
    sync_thread = threading.Thread(target=sync_continuously)
    sync_thread.start()
    status_placeholder.info("⏳ Sync started...")

if st.button("🛑 Stop Sync") and sync_running:
    sync_running = False
    status_placeholder.warning("🛑 Sync stopped.")
