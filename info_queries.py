#!/usr/bin/python

# info_queries.py
# Created by Disa Mhembere on 2013-08-04.
# Email: disa@jhu.edu
# Copyright (c) 2013. All rights reserved.

import MySQLdb
from contextlib import closing
import sys
from utils.gen_util import sql_escape_string
from getpass import getpass

# All authargs are db auth info i.e db_name, db_host, db_pass, db_user

def get_db_names(**authargs):
  """
  Get all the database names visible to connected user
  """
  return "SHOW DATABASES;"

def create_db(**authargs):
  """
  Create a database
  """
  print "Warning: If you are a mysql user with insufficient permissions this operation will fail."
  print "Would you like to change users Y/N ?"
  if sys.stdin.readline().lower().strip() == 'y':
    print "Enter the user account you would like to use:"
    authargs["db_user"] = sys.stdin.readline().lower.strip()
    authargs["db_pass"] = getpass("Enter password for '%s':" % authargs["db_user"])

  print "Enter the name of the database you would like to create:"
  new_db_name = sys.stdin.readline()

  db = MySQLdb.connect(host=authargs["db_host"], user=authargs["db_user"], passwd=authargs["db_pass"], db=authargs["db_name"])
  db.autocommit(True)

  with closing(db.cursor()) as cursor:
    cursor.connection.autocommit(True)
    try:
      cursor.execute("CREATE DATABASE %s" % sql_escape_string(new_db_name))
    except Exception, msg:
      sys.stderr.write("ERROR: Database operation failure!")
      return "SELECT ERROR: %s" % msg[1]

  return "SELECT 'New table \'%s\' successfully created!';" % new_db_name

def enter_db(**authargs):
  """
  Enter a database to inspect further
  """
  print "Enter the name of the database you want to enter:"
  return "USE %s;" % (sql_escape_string(sys.stdin.readline()))

def get_table_description(**authargs):
  """
  Show column/fields & their types from a specific table
  """
  print "Enter table name:"
  tb_name = sql_escape_string(sys.stdin.readline())

  return "SHOW COLUMNS FROM %s" % tb_name

def get_table_names(**authargs):
  """
  Get the names of all the tables in the database selected.
  """
  return "SHOW TABLES;"

def get_datadir(**authargs):
  """
  Show the data directory
  """
  return "SHOW VARIABLES LIKE 'datadir';"

def get_sample_rows(**authargs):
  """
  Get a couple of sample rows to see what your data looks lis
  """
  print "Enter table name:"
  tb_name = sql_escape_string(sys.stdin.readline())
  return "SELECT * FROM %s.%s LIMIT 2;" % (authargs["db_name"], tb_name)

def grant_user_access(access_item, **authargs):
  """
  Give a user privileges to perform some actions

  positional args:
  ===============
  access_item : the item the user will now have permission to access
  """
  # TODO
  pass

def main():
  print "No main implemented for", __file__


if __name__ == '__main__':
  main()