@echo off
title Package install
set /p "pkg=Enter pkg name: "
python panel.py pkg -p %pkg%
timeout 3
exit