#!/bin/bash

# Created by Disa Mhembere on 2013-08-01.
# Email: disa@jhu.edu
# Copyright (c) 2013. All rights reserved.

# Incomplete setup helper for mysql user permissions. TODO: Complete this

unamestr=`uname`
if [[ "$unamestr" == 'Linux' ]]; then
  sudo useradd -G wheel mysql

elif [[ "$unamestr" == 'Darwin' ]]; then
  sudo dseditgroup -o edit -a mysql -t user staff
fi

