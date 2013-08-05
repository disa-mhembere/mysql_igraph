#!/usr/bin/python

# gen_util.py
# Created by Disa Mhembere on 2013-08-02.
# Email: disa@jhu.edu
# Copyright (c) 2013. All rights reserved.

# General helpers for ingest and time slicing

def sql_escape_string(string):
  """
  Escape any string that will cause errors with sql when used as a name of anything

  positional args:
  ================
  string: the string that should be checked

  returns:
  ========
  string escaped
  """

  import re

  if bool(re.compile(r'[^a-z0-9.]').search(string)):
    return "`" + string + "`"
  else:
    return string


def get_date_type(date_string):
  """
  Determine if the date_string is a matching date type

  positional args:
  ===============
  date_string: the sting that may be date/time type

  returns:
  =======
  string with the type determined

  raises:
  =======
  ValueError for non format matching
  """
  from time import strptime

  date_string = date_string.strip() # JIC

  date_type = None
  try:
    strptime(date_string, "%Y-%m-%d %H:%M:%S")
    date_type = "datetime"
  except :
    try:
      strptime(date_string, "%Y-%m-%d")
      date_type = "date"
    except:
      # This will raise exception if fails
      strptime(date_string, '%H:%M:%S')
      date_type = "time"

  return date_type

def test():
  # TODO TESTS
  pass


if __name__ == '__main__':
  test()