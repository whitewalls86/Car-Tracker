@echo off
cd /d "C:\Users\mille\PycharmProjects\Car Tracker"
call .venv\Scripts\activate
call .venv\Scripts\python.exe main.py >> logs\scrape_log.txt 2>&1
