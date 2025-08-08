@echo off
echo Starting SCADA Sync Streamlit App...
echo ---------------------------------------

:: Navigate to the project folder (update path if needed)
cd /d "%~dp0"

:: Set Python path if needed, otherwise uses default
set PYTHON_EXEC=python

:: Install required Python packages
echo Installing dependencies...
%PYTHON_EXEC% -m pip install --upgrade pip
%PYTHON_EXEC% -m pip install -r requirements.txt

:: Start Streamlit app and pass the folder path as an argument
echo Launching app...
%PYTHON_EXEC% -m streamlit run sqlserver_to_postgres_app.py "C:\Program Files (x86)\Microsoft SQL Server\MSSQL12.SQLEXPRESS\MSSQL\Backup"

pause
