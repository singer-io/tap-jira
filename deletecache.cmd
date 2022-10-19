rd /s /q build
rd /s /q dist
rd /s /q C:\code\Python3.10\Lib\site-packages\tap_jira-2.1.4-py3.10.egg
rem Drop other files?
pause
python setup.py install