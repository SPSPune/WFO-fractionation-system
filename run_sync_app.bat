@echo off
cd /d "G:\Monika\WFO Fractionation System\scada-analytics-platform"
echo Starting Streamlit app...
python -m streamlit run sqlserver_to_postgres_app.py
pause
