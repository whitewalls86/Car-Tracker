@echo off
cd /d "C:\Users\mille\PycharmProjects\Car Tracker"

:: Activate virtual environment
call .venv\Scripts\activate.bat

:: Create logs directory if it doesn't exist
if not exist logs mkdir logs

:: Run the dashboard and write logs
streamlit run dashboard.py >> logs\dashboard_log.txt 2>&1