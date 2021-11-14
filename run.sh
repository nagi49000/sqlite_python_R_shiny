#!/bin/bash


export PYTHONPATH=${PYTHONPATH}:`pwd`/python
python python/sql_server/server.py & # spawn sql wrapper server
cd R/client
Rscript run_app.R # spawn front end UI server
