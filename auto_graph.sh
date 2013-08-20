#!/bin/bash
# Personal tester script

python ingest.py $1 '\t' -f -np

filename=$(basename $1)
tablename="${filename%.*}"

python slice.py -t $tablename -np -n $2 -tc time -sc src -dc dest -wc amt -o pickle
