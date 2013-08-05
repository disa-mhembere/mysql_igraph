#!/usr/bin/python

# mysql_setup.py
# Created by Disa Mhembere on 2013-08-05.
# Email: disa@jhu.edu
# Copyright (c) 2013. All rights reserved.

import argparse
from contextlib import closing
import MySQLdb
from getpass import getpass

def setup(py_db_name, **authargs):
  print "Connecting to database %s ..." % authargs["db_name"]
  db = MySQLdb.connect(host=authargs["db_host"], user=authargs["db_user"], passwd=authargs["db_pass"])
  db.autocommit(True)

  with closing( db.cursor() ) as cursor:
    cursor.connection.autocommit(True)

    cursor.execute("SELECT EXISTS(SELECT 1 FROM mysql.user WHERE user = 'python');")
    if cursor.fetchone()[0] == 0: # Means there is no python user in MySQL
      print "'python' user not found. Creating new 'python' user ..."
      cursor.execute("CREATE USER 'python'@'%s' IDENTIFIED BY '%s';" % (authargs["db_host"], getpass("Please enter a password for the 'python user. Hit enter/return for no password:'")))

    # Check if db exists
    if not (cursor.execute("SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = '%s';" % py_db_name)):
      cursor.execute("CREATE DATABASE %s;" % py_db_name)
    else:
      print "Database '%s' already exists. It will not be recreated." % py_db_name

    # Grant privileges
    cursor.execute("GRANT ALL ON %s.* TO 'python'@'%s';" % (py_db_name, authargs["db_host"]))

  print "Setup successfully completed. You are now ready to use the mysql_igraph extension"

def main():

  parser = argparse.ArgumentParser(description='Setup script to use of mysql_igraph extension. Creates python user if none exists & sets up a database for use.')

  parser.add_argument("--db_user", "-u", action="store", defualt="root", help="A user with sufficient permission to create accounts and grant priveleges. Default is 'root'")
  parser.add_argument("--db_host", "-H", action="store", default="localhost", help="The database hostname/network address. Default is localhost.")
  parser.add_argument("--no_pass", "-np", action="store_true", help="Pass the flag if your 'python' user has no password")

  parser.add_argument("--py_db_name", "-d", action="store", default="Pydb", help="The database name that you want to create to hold mysql igraph graphs.")

  result = parser.parse_args()

  if result.no_pass:
    result.db_pass = ""
  else:
    result.db_pass = getpass("Please enter the '%s' user password for MySQL:" % result.db_user)

  print "Attempting connection as user: %'s'@'%s'" % (result.db_user, result.db_host)
  print "Attempting to create database: %s" % (result.db_name)

  authargs = {"db_user":result.db_user, "db_host": result.db_host, "db_pass": result.db_pass}
  setup(result.py_db_name, **authargs)

if __name__ == '__main__':
  main()