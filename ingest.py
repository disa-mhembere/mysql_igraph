#!/usr/bin/python

# ingest.py
# Created by Disa Mhembere on 2013-07-19.
# Email: disa@jhu.edu
# Copyright (c) 2013. All rights reserved.

import argparse
import os, sys
from time import time
from subprocess import call
from contextlib import closing
import MySQLdb
from exceptions import RuntimeError
from utils import ingest_util
from utils import gen_util

def data_to_db(sv_file_fn, separator, db_name, tb_name, headers, coltypes, force, passwd, ignore_cols=None):
  """
  * Specifically for bitcoin data at this point
  Convert a tsv file to a csc matrix

  positional args:
  ===============
  sv_file_fn : * separated values file

  optional args:
  ==============
  args: the mysql data types of each column
  """
  start = time()

  separator_dict = {"\\t":"\t",  "\'": "'", "\"": '"', "\\f":"\f", "\\v": "\v"} # Essentially unescape escaped characters
  if separator_dict.has_key(separator):
    separator = separator_dict[separator] # Map escaped characters

  if tb_name is None:
    tb_name = os.path.splitext(sv_file_fn.replace('\\','/').split('/')[-1])[0]

  ignore_lines = 0 # The number of lines to ignore before we get to data
  if headers == 'auto':
    ignore_lines = 1
    sv = open(sv_file_fn, 'rb')
    print "Auto extract of headers ..."
    headers = sv.readline().strip().split(separator)

  if not coltypes:
    try:
      print "Auto extract of column data types ..."
      sample_row = sv.readline().strip().split(separator)
    except Exception:
      sv = open(sv_file_fn, 'rb')
      sv.readline()
      try:
        sample_row = sv.readline().strip().split(separator)
      except Exception:
        raise RuntimeError("The input file must have at least 2 lines of values in it!\n")

  if not (len(headers) == len(sample_row)):
    sys.stderr.write("""The number of headers does not match the number of columns. Check command line args and/or the data file.\n""")
    sys.exit(-1)

  if ignore_cols is not None:
    # fix headers
    for idx, item in enumerate(headers):
      if idx in ignore_cols:
        headers[idx] = "@dummy"

  # Determine column types
  coltypes = ingest_util.deduce_type(sample_row)

  qry_create_table = "CREATE TABLE %s ( "

  for idx, header in enumerate(headers):
    if not headers[idx] == "@dummy":
      headers[idx] = gen_util.sql_escape_string(header)
      qry_create_table += headers[idx] + "  " + coltypes[idx] + ", "

  qry_create_table = qry_create_table[:-2]
  qry_create_table += ");"

  # connect
  print "Connecting to database %s ..." % db_name
  db = MySQLdb.connect(host="localhost", user="python", passwd=passwd, db=db_name)
  db.autocommit(True)

  with closing( db.cursor() ) as cursor:
    cursor.connection.autocommit(True)

    print "Attempting to Creating table '%s' ..." % tb_name
    if force:
      cursor.execute("DROP TABLE IF EXISTS %s;" % tb_name)
      print "'--force' flag used. If table '%s' exists it was dropped and recreated ..." % tb_name

    tb_name_counter = 1
    while True:
      try:
        cursor.execute( qry_create_table % (tb_name))
        print "Table %s created ..." % tb_name
        break # Only gets here on a successful table create
      except Exception, msg:
        if msg.args[0] == 1050: # 1050 is : "Table 'tb_name' already exists"
          print "Table %s already present. Adapting table name ..." % tb_name
          tb_name = ingest_util.adapt_table_name(tb_name)
        else:
          raise Exception(msg)

    try:
      cursor.execute("FLUSH TABLES;")
    except:
      sys.stderr.write("[WARNING]: Insufficient permission to run command 'FLUSH TABLES'. This is will slow down insert speed.\
                     Give the 'python' process permission with MySQL to avoid this warning.\n")

    cursor.execute("SHOW VARIABLES LIKE 'datadir';")
    tb_dir = cursor.fetchone()[1]

    # may fail with incorrect level of permissions for python
    call(["myisamchk", "--keys-used=0", "-rq", "%s/%s" % (os.path.join(tb_dir,db_name), tb_name) ])

    insert_stmt = ("""
        LOAD DATA LOCAL INFILE '%s'
        INTO TABLE %s
        FIELDS TERMINATED BY '%s'
        LINES TERMINATED BY '\\n'
        IGNORE %d LINES
        ("""
        % (os.path.abspath(sv_file_fn), tb_name, separator, ignore_lines))

    # Add header names to insert statement
    # todo add ignore headers/column which will have @dummy value
    for col in headers:
      insert_stmt += col + ","
    insert_stmt = insert_stmt[:-1] + ");"

    cursor.execute(insert_stmt)

    # may fail with incorrect level of permissions for python
    call(["myisamchk","-rq", "%s/%s" % (os.path.join(tb_dir,db_name), tb_name)]) # Python needs permissions for this

    try:
      cursor.execute("FLUSH TABLES;")
    except:
      sys.stderr.write("[WARNING]: Insufficient permission to run command 'FLUSH TABLES'. This is will slow down insert speed.\
                        Give the 'python' process permission with MySQL to avoid this warning.\n")

  print "Success! Time taken to parse and ingest entries: %.3f sec" % (time() - start)
  print "Final table name: '{0}'.".format(tb_name)

  db.close() # close db connection
  return db_name, tb_name


def main():
  parser = argparse.ArgumentParser(description="Insert a .*sv file into a MySQL database")
  parser.add_argument("sv_file_fn", action="store", help="The full file name of the sv file")
  parser.add_argument("separator", action="store", help="The kind of separator between columns in the data file. Each line must end with a newline/carriage return\
                      Surrounded by quotes e.g '\t' for tab. ' ' for space. ',' for 'comma'. The following and more are valid: '@', '#', '~' etc.. '\\n' IS NOT!")
  parser.add_argument("--database", "-d", action="store", default="Pydb", help="The name of the database where table with the graph will be held")
  parser.add_argument("--tb_name", "-t", action="store", help="The name you want the table to have in the db")
  parser.add_argument("--headers", "-H", action="store", default="auto", nargs="+", help="If file does not have headers (column titles/labels) in the first line of the *sv file\
                      -- use this flag  to specify in e.g -H source destination time attr1 'attr w space' att3. If input file does have headers & you want to use them DO NOT use this flag")
  parser.add_argument("--coltypes", "-ct", action="store", nargs="+", help="If not used -- types are assigned automatically based on auto detection of type using the 2nd line of data.\
                      MySQL types of the each column enclosed 'in quotes'. As many as there are columns in the data.\
                      see http://dev.mysql.com/doc/refman/5.0/en/data-type-overview.html for acceptable types. E.g 'integer not null'. 'double', 'FLOAT', varchar(64) etc..")
  parser.add_argument("--force", "-f", action="store_true", help="Force table name to be what you want (DROPS TABLE WITH SAME NAME IF PRESENT) or create new table with suffix")
  parser.add_argument("--no_pass", "-np", action="store_true", help="Pass the flag if your 'python' user has no password")
  parser.add_argument("--ignore_cols", "-i", action="store", nargs="+", type=int, help="Index of columns to ignore. O-based indexing. E.g -i 0 3 5. This will not insert columns 0,3 and 5 into the DB.")

  result = parser.parse_args()

  if result.no_pass:
    result.db_pass = ""
  else:
    from getpass import getpass
    #result.db_pass = "python"
    result.db_pass = getpass("Please enter the 'python' user password for MySQL:") # TODO: UNCOMMENT

  db_name, tb_name = data_to_db(result.sv_file_fn, result.separator, result.database, result.tb_name, result.headers, result.coltypes, result.force, result.db_pass, result.ignore_cols)
if __name__ == "__main__":
  main()