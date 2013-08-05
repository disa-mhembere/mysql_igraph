#!/bin/bash

# scaling_test.sh 
# Created by Disa Mhembere on 2013-08-05.
# Email: disa@jhu.edu
# Copyright (c) 2013. All rights reserved.

# Script to test the speed of time-series graph generation

# Take in some cmd line args
TABLE_NAME=$1
DATABASE_NAME=$2
TIME_ATTR=$3
SRC_ATTR=$4
DEST_ATTR=$5
WEIGHT_ATTR=$6
OUTPUT_FMT=$7

TEST_STAMP=$(date +%s)
RESULT_FN="test_result_$TEST_STAMP.log"

echo "Running scalability test at $(date)"

for slice in 1 2 4 8
do
  echo "Working on slice $slice ..."
  python slice.py -T $TABLE_NAME -t time_attr_name -d $DATABASE_NAME -S "test_$(date)" -n $slice -sc $SRC_ATTR -dc $DEST_ATTR -w $WEIGHT_ATTR -o $OUTPUT_FMT >> $RESULT_FN
done

echo "Test complete!"
