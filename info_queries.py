#!/usr/bin/python

# info_queries.py
# Created by Disa Mhembere on 2013-08-04.
# Email: disa@jhu.edu
# Copyright (c) 2013. All rights reserved.

import MySQLdb
from contextlib import closing
import sys
from utils.gen_util import sql_escape_string

# All kwargs are db auth info i.e db_name, db_host, db_pass, db_user

def get_table_description(**kwargs):
  print "Enter table name:"
  tb_name = sql_escape_string(sys.stdin.readline())

  return "show columns from %s" % tb_name

def get_table_names(**kwargs):
  """
  Get the names of all the tables in the database selected.
  """
  return "show tables;"

def get_datadir(**kwargs):
  """
  Show the data directory
  """
  return "show variables like 'datadir';"

def get_sample_rows(**kwargs):
  """
  Get a couple of sample rows to see what your data looks lis
  """
  print "Enter table name:"
  tb_name = sql_escape_string(sys.stdin.readline())
  return "select * from %s.%s limit 2;" % (kwargs["db_name"], tb_name)

def main():
  print "No main implemented for", __file__


if __name__ == '__main__':
  main()