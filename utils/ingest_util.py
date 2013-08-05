#!/usr/bin/python

# ingest_util.py
# Created by Disa Mhembere on 2013-07-30.
# Email: disa@jhu.edu
# Copyright (c) 2013. All rights reserved.

# Bunch of helper functions that are used in MySQL-igraph extension for time series graphs

from gen_util import get_date_type

def deduce_type(sample_row):
  """
  Try to figure out the SQL types for a row of data

  positional args:
  ===============
  sample_row - a sample row of the real data

  returns:
  ========
  A list containing strings with the deduced types

  """

  from time import strptime

  coltypes = []
  for sample in sample_row:
    try:
      col_val = int(sample)
      if (col_val + col_val) >= 2147483647:
        coltypes.append("bigint")
      else:
        coltypes.append("integer")
    except:
      try:
        float(sample)
        coltypes.append("double") # default to higher bit representation. Could have been float
      except:
        # Need to support all mysql date types:
        # DATE ==> '1000-01-01 00:00:00' = datetime. '1001-01-01' = date. '838:59:59' = time. '2012' = year # Only support 4 year years
        try:
          coltypes.append(get_date_type(sample))
        except:
          # Now parse field and try to match
          if (sample.strip().lower() == "true" or sample.strip().lower == "false"):
            coltypes.append("boolean")
          else:
            coltypes.append("text")

    print "Column sample '%s' type deduced as sql type '%s' .." % (sample, coltypes[-1])
  return coltypes

def adapt_table_name(tb_name):
  '''
  If the table name already exists take a name close to
  the requested one just as file systems do with files
  named the same in a directory

  positional args:
  ================
  tb_name : the name of the table that already exists

  returns:
  =======
  string : the new name of the table with a prefix added
  '''
  import re

  patt = re.compile(r"(^.*_)(\d)+$") # This matches 'tb_name_1' but not 'tb_name'
  m = re.match(patt, tb_name)
  if m:
    tb_name = m.group(1) + str(int(tb_name.split("_")[-1]) + 1)
  else:
    tb_name += "_1" # first suffix
  return tb_name

def test():
  # adapt_table_name tests
  assert(adapt_table_name("rando_mtable_434") == "rando_mtable_435")
  assert(adapt_table_name("_mtable_4539") == "_mtable_4540")
  assert(adapt_table_name("Customer file") == "Customer file_1")
  assert(adapt_table_name("test_33.4") == "test_33.4_1")
  print "adapt_table_name tests successful with no errors ..."

  #deduce_type tests
  # TODO ADD TESTS HERE

if __name__ == '__main__':
  test()