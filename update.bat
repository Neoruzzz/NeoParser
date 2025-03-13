@echo off
title Update bot
python panel.py stop
python panel.py install
python panel.py start
timeout 3
exit