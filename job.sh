#!/bin/sh

echo "Populating experiments on lines table"
python src/App.py -l

sleep 5m

echo "Populating experiments on specimen table"
python src/App.py -e

sleep 5m

echo "Populating images table"
python src/App.py -i

echo "All jobs are done"
 