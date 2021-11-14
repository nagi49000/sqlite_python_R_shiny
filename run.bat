SET PYTHONPATH=%PYTHONPATH%;%cd%\python
start python .\python\sql_server\server.py
cd R\client
Rscript run_app.R
