import os
import streamlit as st
import psycopg2
import pandas as pd
from datetime import datetime
from streamlit_autorefresh import st_autorefresh

# Auto-refresh every 60 seconds
st_autorefresh(interval=60 * 1000, key="datarefresh")

st.set_page_config(page_title="SQL File to PostgreSQL Sync", layout="centered")
st.title("🔄 SCADA SQL File to PostgreSQL Data Sync")

# Tag Index Mapping
tag_mapping = {
    251: "TI-31", 253: "TI-32", 254: "TI-33", 255: "TI-34", 256: "TI-35",
    257: "TI-36", 258: "TI-37", 259: "TI-38", 260: "TI-39", 261: "TI-40",
    270: "TI-41", 271: "TI-42", 273: "TI-43", 274: "TI-44", 275: "TI-45",
    272: "TI-42A", 279: "TI-54", 199: "TI-107", 201: "TI-109", 296: "TI-73A",
    297: "TI-73B", 280: "TI-55", 154: "PTT-03", 149: "PTB-03", 122: "LT-O5",
    123: "LT-06", 28: "FT-01", 46: "FT-07", 63: "FT-10", 37: "FT-04"
}

# ---------- SCADA SQL File Configuration ----------
st.header("📂 SCADA SQL File Configuration")
sql_file_folder = st.text_input("Enter SCADA SQL File Folder Path (e.g., D:/scada/sql_output)")

# ---------- PostgreSQL Credentials ----------
st.header("🔐 PostgreSQL Credentials")
db_name = st.text_input("Database Name", value="scada_db")
db_user = st.text_input("Username", value="postgres")
db_password = st.text_input("Password", type="password")
db_host = st.text_input("Host", value="localhost")
db_port = st.text_input("Port", value="5432")

# ---------- Sync Logic ----------
if sql_file_folder and db_name and db_user and db_password:
    if os.path.exists(sql_file_folder):
        files = [f for f in os.listdir(sql_file_folder) if f.endswith('.sql')]
        if files:
            latest_file = max(files, key=lambda x: os.path.getctime(os.path.join(sql_file_folder, x)))
            latest_file_path = os.path.join(sql_file_folder, latest_file)
            st.success(f"📄 Found latest SQL file: `{latest_file}`")

            try:
                # Read raw SQL file into dataframe
                with open(latest_file_path, 'r') as f:
                    sql_commands = f.read()

                # Execute SQL script and load into temporary table
                conn = psycopg2.connect(
                    dbname=db_name,
                    user=db_user,
                    password=db_password,
                    host=db_host,
                    port=db_port
                )
                cur = conn.cursor()
                cur.execute(sql_commands)
                conn.commit()

                # Read new data from temp table (assuming temp table name = scada_raw)
                df = pd.read_sql("SELECT \"DateAndTime\", \"TagIndex\", \"Val\" FROM scada_raw", conn)

                # Filter for only known tags
                df = df[df["TagIndex"].isin(tag_mapping.keys())]
                df["Tag"] = df["TagIndex"].map(tag_mapping)
                df_pivot = df.pivot(index="DateAndTime", columns="Tag", values="Val").reset_index()

                # Create final table if not exists
                column_defs = ", ".join([f'"{col}" DOUBLE PRECISION' for col in df_pivot.columns if col != "DateAndTime"])
                create_table_sql = f"""
                    CREATE TABLE IF NOT EXISTS scada_data (
                        "DateAndTime" TIMESTAMP PRIMARY KEY,
                        {column_defs}
                    )
                """
                cur.execute(create_table_sql)
                conn.commit()

                # Insert or update
                for _, row in df_pivot.iterrows():
                    columns = ', '.join(f'"{col}"' for col in df_pivot.columns)
                    values = ', '.join(['%s'] * len(df_pivot.columns))
                    insert_sql = f"""
                        INSERT INTO scada_data ({columns}) VALUES ({values})
                        ON CONFLICT ("DateAndTime") DO UPDATE SET
                        {', '.join(f'"{col}" = EXCLUDED."{col}"' for col in df_pivot.columns if col != "DateAndTime")}
                    """
                    cur.execute(insert_sql, tuple(row))

                conn.commit()
                cur.close()
                conn.close()

                st.success("✅ SCADA data successfully loaded and pivoted into PostgreSQL!")

            except Exception as e:
                st.error(f"❌ Error during SQL execution or transformation: {e}")
        else:
            st.warning("⚠️ No SQL files found in the provided folder.")
    else:
        st.error("❌ Provided folder path does not exist.")
else:
    st.info("ℹ️ Please fill in all required fields to start syncing.")

# ---------- Create New DB UI ----------
st.header("🛠️ Optional: Create New PostgreSQL Database")
create_db_name = st.text_input("New Database Name", key="create_db_name")
create_db_user = st.text_input("PostgreSQL Username", value="postgres", key="create_db_user")
create_db_password = st.text_input("PostgreSQL Password", type="password", key="create_db_password")
create_db_host = st.text_input("PostgreSQL Host", value="localhost", key="create_db_host")
create_db_port = st.text_input("PostgreSQL Port", value="5432", key="create_db_port")

if st.button("➕ Create Database"):
    try:
        default_conn = psycopg2.connect(
            dbname="postgres",
            user=create_db_user,
            password=create_db_password,
            host=create_db_host,
            port=create_db_port
        )
        default_conn.autocommit = True
        cur = default_conn.cursor()
        cur.execute(f"SELECT 1 FROM pg_database WHERE datname = '{create_db_name}'")
        exists = cur.fetchone()

        if exists:
            st.warning(f"⚠️ Database `{create_db_name}` already exists.")
        else:
            cur.execute(f"CREATE DATABASE {create_db_name}")
            st.success(f"✅ Database `{create_db_name}` created successfully.")

        cur.close()
        default_conn.close()
    except Exception as e:
        st.error(f"❌ Error creating database: {e}")
