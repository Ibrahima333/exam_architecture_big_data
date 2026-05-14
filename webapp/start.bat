@echo off
setlocal enabledelayedexpansion

REM Set default model path if not defined
if "%MODEL_PATH%"=="" set MODEL_PATH=\model\model.pkl

REM Check if model file exists
if not exist "%MODEL_PATH%" (
    echo Modele introuvable: %MODEL_PATH%
    exit /b 1
)

REM Start Flask application
echo Demarrage de Fraude-Signal...
python app.py