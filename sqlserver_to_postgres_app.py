import pandas as pd
import sqlalchemy
import psycopg2
import streamlit as st
from datetime import datetime, timedelta
from streamlit_autorefresh import st_autorefresh

# --- CONFIG ---
MSSQL_CONN_STR = "mssql+pyodbc://username:password@SERVER/SCADA_DB?driver=ODBC+Driver+17+for+SQL+Server"
PGSQL_DB_NAME = "distillation_scada"
PGSQL_CONN_STR = f"postgresql+psycopg2://postgres:password@localhost:5432/{PGSQL_DB_NAME}"
SCADA_TABLE_NAME = "scada_raw_data"
DEST_TABLE = "scada_cleaned_wide"

# --- TAG MAPPING ---
TAG_MAP = {
    251: "TI-31", 253: "TI-32", 254: "TI-33", 255: "TI-34", 256: "TI-35", 257: "TI-36",
    258: "TI-37", 259: "TI-38", 260: "TI-39", 261: "TI-40", 270: "TI-41", 271: "TI-42",
    273: "TI-43", 274: "TI-44", 275: "TI-45", 272: "TI-42A", 279: "TI-54", 199: "TI-107",
    201: "TI-109", 296: "TI-73A", 297: "TI-73B", 280: "TI-55", 154: "PTT-03", 149: "PTB-03",
    122: "LT-05", 123: "LT-06", 28: "FT-01", 46: "FT-07", 63: "FT-10", 37: "FT-04"
}

# --- DB CREATION IF NOT EXISTS ---
def create_pgsql_db():
    conn = psycopg2.connect(dbname="postgres", user="postgres", password="password", host="localhost", port="5432")
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute(f"SELECT 1 FROM pg_database WHERE datname = '{PGSQL_DB_NAME}'")
    if not cur.fetchone():
        cur.execute(f"CREATE DATABASE {PGSQL_DB_NAME}")
    cur.close()
    conn.close()

# --- FETCH SCADA DATA ---
def fetch_recent_scada_data():
    engine = sqlalchemy.create_engine(MSSQL_CONN_STR)
    one_min_ago = datetime.now() - timedelta(minutes=1)
    query = f"""
        SELECT timestamp, tag_index, value
        FROM {SCADA_TABLE_NAME}
        WHERE timestamp >= '{one_min_ago.strftime('%Y-%m-%d %H:%M:%S')}'
    """
    return pd.read_sql(query, engine)

# --- PROCESS AND PIVOT ---
def process_and_pivot(df):
    df["tag"] = df["tag_index"].map(TAG_MAP)
    df.dropna(subset=["tag"], inplace=True)
    return df.pivot_table(index="timestamp", columns="tag", values="value").reset_index()

# --- SAVE TO PGSQL ---
def save_to_pgsql(df):
    engine = sqlalchemy.create_engine(PGSQL_CONN_STR)
    df.to_sql(DEST_TABLE, con=engine, if_exists='append', index=False)

# --- STREAMLIT UI ---
st.set_page_config(page_title="SCADA Data Sync", layout="centered")
st.title("🔄 SCADA to PostgreSQL Sync")

# Autorefresh every 60 seconds
st_autorefresh(interval=60000, key="auto_refresh")

# Init state
if "sync_active" not in st.session_state:
    st.session_state.sync_active = False

# Start/Stop Button
if st.button("▶️ Start Continuous Sync"):
    st.session_state.sync_active = True
    st.success("Sync started. This app will fetch data every minute.")

if st.button("⏹️ Stop Sync"):
    st.session_state.sync_active = False
    st.info("Sync stopped.")

# Sync logic
if st.session_state.sync_active:
    with st.spinner("Checking and syncing SCADA data..."):
        try:
            create_pgsql_db()
            df_raw = fetch_recent_scada_data()
            if df_raw.empty:
                st.warning("No new data in the last minute.")
            else:
                df_cleaned = process_and_pivot(df_raw)
                save_to_pgsql(df_cleaned)
                st.success(f"Saved {len(df_cleaned)} row(s) at {datetime.now().strftime('%H:%M:%S')}")
        except Exception as e:
            st.error(f"❌ Error: {e}")

st.caption("This app checks and saves new SCADA data every minute once started.")
