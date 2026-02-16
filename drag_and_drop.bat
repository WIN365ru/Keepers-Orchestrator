@echo off
REM Drag and drop a torrent file onto this batch file to add it to qBittorrent with a custom save path.
REM You can also run it from the command line: drag_and_drop.bat "path\to\file.torrent"

if "%~1"=="" (
    echo No file provided. Please drag and drop a torrent file onto this script.
    pause
    exit /b
)

python "%~dp0qbit_adder.py" "%~1"
pause
