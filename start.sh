#!/bin/sh

sh clear.sh

python3 -m venv venv
source venv/bin/activate
python3 -m pip install  --upgrade pip
pip install -r requirements.txt	
source venv/bin/activate

chmod 777 main.py
./main.py
