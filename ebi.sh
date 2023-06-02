#!/bin/sh

echo "Populating ebi procedures table"
python src/App.py ebi -p

sleep 5m

echo "Populating ebi images table"
python src/App.py ebi -e

echo "Done"