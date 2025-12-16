@echo off
echo Starting FastAPI backend server...
echo Make sure you are in the project root directory
echo.
python -m uvicorn api.main:app --host 0.0.0.0 --port 8000 --reload
pause

