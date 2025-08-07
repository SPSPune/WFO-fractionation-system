@echo off
cd /d "D:\WFO-fractionation-system"
call scada_env\Scripts\activate
streamlit run scada_ui_app.py
