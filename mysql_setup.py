#!/usr/bin/python

# mysql_setup.py
# Created by Disa Mhembere on 2013-08-05.
# Email: disa@jhu.edu
# Copyright (c) 2013. All rights reserved.

import argparse
from contextlib import closing
import MySQLdb
from getpass import getpass

def setup(new_db_name, new_user, all_access, **authargs):

  db = MySQLdb.connect(host=authargs["db_host"], user=authargs["db_user"], passwd=authargs["db_pass"])
  db.autocommit(True)

  with closing( db.cursor() ) as cursor:
    cursor.connection.autocommit(True)

    cursor.execute("SELECT EXISTS(SELECT 1 FROM mysql.user WHERE user = '%s');" % new_user)
    if cursor.fetchone()[0] == 0: # Means there is no user in MySQL
      print "Creating new '%s' user ..." % new_user
      cursor.execute("CREATE USER '%s'@'%s' IDENTIFIED BY '%s';" % (new_user, authargs["db_host"], getpass("Please enter a password for the '%s' account. Hit enter/return for no password:" % new_user)))

    else:
      print "\n*[Warning]: The user '%s' already exist. No new user added!" % new_user
    # Check if db exists
    if not (cursor.execute("SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = '%s';" % new_db_name)):
      cursor.execute("CREATE DATABASE %s;" % new_db_name)
      print "Database %s successfully created ..." % new_db_name
    else:
      print "\n*[Warning]: Database '%s' already exists. It will not be recreated!\n" % new_db_name

    # Grant privileges
    cursor.execute("GRANT ALL PRIVILEGES ON %s.* TO '%s'@'%s';" % ("*" if all_access else new_db_name, new_user, authargs["db_host"]))

  db.close() # close db connection
  print "Setup successfully completed! You are now ready to use the mysql_igraph extension!\nYou can create new databases using './mi'"

def main():

  parser = argparse.ArgumentParser(description="Setup script to use mysql_igraph extension. Creates requested user/password (if no duplicate user exists) & sets up a database for use.")

  parser.add_argument("--db_user", "-u", action="store", default="root", help="A user with sufficient permission to create accounts and grant priveleges. Default is 'root'.")
  parser.add_argument("--db_host", "-H", action="store", default="localhost", help="The database hostname/network address. Default is 'localhost'.")
  parser.add_argument("--all_access", "-a", action="store_true", help="Pass the flag if you want to allow the new user access to all tables in the database. This permits user to add/delete all databases.")

  parser.add_argument("--new_user", "-n", action="store", default="python", help="The new user you want to create username. Default is 'python'.")
  parser.add_argument("--new_db_name", "-d", action="store", default="Pydb", help="The new database name that you want to create to hold mysql igraph graphs. Default is 'Pydb'.")

  result = parser.parse_args()

  result.db_pass = getpass("Please enter the '%s' user password for MySQL:" % result.db_user)

  print "Attempting connection as user: '%s'@'%s'" % (result.db_user, result.db_host)
  print "Attempting to create database: %s" % (result.new_db_name)

  authargs = {"db_user":result.db_user, "db_host": result.db_host, "db_pass": result.db_pass}
  setup(result.new_db_name, result.new_user, result.all_access, **authargs)

if __name__ == '__main__':
  main()
