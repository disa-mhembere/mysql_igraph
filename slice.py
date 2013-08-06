#!/usr/bin/python

# slice.py
# Created by Disa Mhembere on 2013-07-22.
# Email: disa@jhu.edu
# Copyright (c) 2013. All rights reserved.

import argparse
import MySQLdb
from contextlib import closing
import igraph
import threading
import os
from time import time
from utils import gen_util
import datetime as dt
import sys
from getpass import getpass

def create_graph(tb_name, time_col, save_dir, src_col, dest_col, weight_col,\
                    out_format, num_slices, aggregation, **authargs):

  print "Selected output format: %s ..." % out_format

  # connect
  print "Connecting to database %s ..." % authargs["db_name"]
  db = MySQLdb.connect(host=authargs["db_host"], user=authargs["db_user"], passwd=authargs["db_pass"], db=authargs["db_name"])
  db.autocommit(True)
  process_list = [] # will hold all concurrent processes

  time_col = gen_util.sql_escape_string(time_col)
  src_col = gen_util.sql_escape_string(src_col)
  dest_col = gen_util.sql_escape_string(dest_col)

  begin_time = time()
  with closing(db.cursor()) as cursor:
    cursor.connection.autocommit(True)

    cursor.execute("SELECT MAX(%s) FROM %s" % (src_col, tb_name))
    max_src = cursor.fetchone()[0]

    cursor.execute("SELECT MAX(%s) FROM %s" % (dest_col, tb_name))
    max_dest = cursor.fetchone()[0]

    cursor.execute("SELECT MIN(%s) FROM %s" % (time_col, tb_name))
    min_time = cursor.fetchone()[0]

    cursor.execute("SELECT MAX(%s) FROM %s" % (time_col, tb_name))
    max_time = cursor.fetchone()[0]

    # Figure out chronological item type
    cursor.execute("DESCRIBE %s %s;" % (tb_name, time_col))
    chron_type = cursor.fetchone()[1]

  # Figure out intervals
  interval_tuples = [] # 2-item tuples: (start, end) e.g if we have 3 slices there will 3, 2-element tuples

  delta = (max_time - min_time)/num_slices
  initial_time = min_time

  for slce in range(num_slices):
    final_time = initial_time + delta # End last point in the interval
    if chron_type == "time":
      interval_tuples.append((str((dt.datetime.min + initial_time).time()), str((dt.datetime.min + final_time).time())))
    else:
      interval_tuples.append((str(initial_time), str(final_time)))
    #print "Interval %d ==> Begin:" % slce, str(interval_tuples[-1][0]), " End:", str(interval_tuples[-1][1]) # test echo
    initial_time = final_time # The end of the 1st time interval is the beginning of the next

  for idx, tuple_pair in enumerate(interval_tuples):
    with closing(db.cursor()) as cursor:
      cursor.connection.autocommit(True)

      start = time()
      # figure out max node value so I know how big graph is. Graphs are vertex aligned to original
      cursor.execute("SELECT MAX(%s) FROM %s WHERE %s BETWEEN '%s' AND '%s'" % (src_col, tb_name, time_col, str(tuple_pair[0]), str(tuple_pair[1])))
      max_src = cursor.fetchone()[0]

      cursor.execute("SELECT MAX(%s) FROM %s where %s BETWEEN '%s' AND '%s'" % (dest_col, tb_name, time_col, str(tuple_pair[0]), str(tuple_pair[1])))
      max_dest = cursor.fetchone()[0]

      dim = max(max_src, max_dest) + 1 # for 0-based indexing

      # This aggregates the weights & leaves 1 number for the egde in the interval
      if weight_col:
        query_stmt = "SELECT %s, %s, %s(%s) FROM %s WHERE %s BETWEEN '%s' AND '%s' GROUP BY %s, %s"\
                      % (src_col, dest_col, aggregation, weight_col, tb_name, time_col, str(tuple_pair[0]), str(tuple_pair[1]), src_col, dest_col)
      else:
        query_stmt = "SELECT %s, %s, FROM %s WHERE %s BETWEEN '%s' AND '%s' GROUP BY %s, %s"\
                      % (src_col, dest_col, tb_name, time_col, str(tuple_pair[0]), str(tuple_pair[1]), src_col, dest_col)

      print "Querying for edges slice %d took %.f sec ..." % (idx, (time()-start))

      # TODO: Write with MySQL to where I want
      # TODO: Only spawn as many threads as available on system : os.sysconf("SC_NPROCESSORS_ONLN")
      # TODO: Make "continuous" work
      # Determine output format and deliver
      if (out_format in ["csv", "edgelist", "tsv"]):
        start = time()
        print "\nCreating graph for slice %d" % (idx)
        fn = os.path.abspath(os.path.join(save_dir, tb_name+"_slice%d.%s"%(idx, out_format)))

        if num_slices == 1: # serial case
          #db_write_file(fn, query_stmt, out_format, authargs)
          db_write_file(tb_name+"_slice%d.%s"%(idx, out_format), query_stmt, out_format, authargs)

        else: # parallel case
          #thr = threading.Thread(target=db_write_file, args=(fn, query_stmt, out_format, authargs))
          thr = threading.Thread(target=db_write_file, args=(tb_name+"_slice%d.%s"%(idx, out_format), query_stmt, out_format, authargs))
          thr.start()
          process_list.append(thr)
          print "Writing edge slice %d took %.3f sec ..." % (idx, (time()-start))

      elif  (out_format in ["dot", "gml", "graphml", "gw", "lgl", "ncol", "net", "pickle", "picklez", "svg", "leda", "lgr","graphviz", "graphmlz", "pajek"]):
        if num_slices == 1: # serial case
          print "\nPrint launching single graph build..."
          build_igraph_from_db(dim, query_stmt, out_format, authargs, os.path.join(save_dir, tb_name+".%s"%out_format))
        else: # parallel case
          print "\nPrint launching new graph building process %d ..." % idx

          thr = threading.Thread(target=build_igraph_from_db, args=(dim, query_stmt, out_format, authargs, os.path.join(save_dir, tb_name+"%d.%s"%(idx,out_format)),))
          thr.start()
          process_list.append(thr)

      else:
        from exceptions import ValueError
        raise ValueError("The graph output type '%s' is not supported." % out_format)

    for proc in process_list:
      proc.join() # Finish all processes before I exit

  print "All processing complete! Total time for %d slice(s): %.3f" % (idx+1, time()-begin_time)

def db_write_file(out_fn, tb_name, out_format, authargs):
  """
  Use database to write a file to disk with a representation of a graph.

  positinal args:
  ==============
  out_fn: the file name the resulting file will have
  tb_name: the table name from which graph will be created
  out_format: the output format e.g csv, dot, gml ...
  authargs: dict containing database name, password, host

  returns:
  =======
  exit code from db operations
  """

  terminator = {"csv":",", "tsv":"\\t", "edgelist":" "}

  query = """
  SELECT * INTO OUTFILE '%s'
  FIELDS TERMINATED BY '%s' OPTIONALLY ENCLOSED BY '"'
  LINES TERMINATED BY '\n'
  FROM ((%s) AS slice); """ % (out_fn, terminator[out_format], tb_name)

  db = MySQLdb.connect(host=authargs["db_host"], user=authargs["db_user"], passwd=authargs["db_pass"], db=authargs["db_name"])
  db.autocommit(True)

  start = time()
  with closing(db.cursor()) as cursor:
    cursor.connection.autocommit(True)

    cursor.execute(query)
    print "Time to write graph to disk: %.3f" % (time()-start)
    return cursor

def build_igraph_from_db(dim, query_stmt, out_format, authargs, save_fn=None):
  """
  Build an igraph from a database table/query and save in some format

  positinal args:
  ==============
  dim: The dimension of the adjacency matrix. The matrix will be of size dim X dim
  save_fn: the file name you would like the graph saved under. If you don't want to save do not define this
  out_format: the output format e.g dot, gml, leda, pajek etc ...
  authargs: dict containing database name, password, host

  returns:
  =======
  exit code from db operations
  """

  db = MySQLdb.connect(host=authargs["db_host"], user=authargs["db_user"], passwd=authargs["db_pass"], db=authargs["db_name"])
  db.autocommit(True)

  with closing(db.cursor() ) as cursor:
    cursor.connection.autocommit(True)
    start = time()
    print "Getting rows for graph slice ...."
    cursor.execute(query_stmt)
    print "Time to query db for slice: %.8f" % (time()-start)

    start = time()
    rows = cursor.fetchall() # get all rows in the current slice
    print "Time fetch all rows for slice: %.8f" % (time()-start)

  print "Graph build begin for '%s' with dim = %d and rows = %d ..." % (save_fn, dim, len(rows))

  graph = igraph.Graph(n=dim, directed=True, edge_attrs={"weight":0})

  edges = [] # hold all edge-pairs (2-tuple elements in a list) to be added to graph
  edge_weights = [] # Array with same order of `edges` but contains all edge weights

  begin =time()
  start = time()
  for row in rows:
    edges.append((row[0], row[1]))
    edge_weights.append(row[2])

  print "Time taken to extract edges and weights: %.3f sec" % (time()-start)

  print "Populating igraph ..."
  start = time()

  graph.add_edges(edges) # Add all edges at once
  print "Time to add edges: %.3f sec ..." % (time()-start)

  start = time()
  graph.es["weight"] = edge_weights # Assign all edges their weights
  print "Time to add edges weights: %.3f sec ..." % (time()-start)

  print "Graph (order, size): (%d, %d)" % (graph.vcount(), graph.ecount())
  #print "Graph is diameter:", graph.diameter()
  #print "Graph transitivity:", graph.transitivity_undirected()

  if save_fn:
    if not os.path.exists(os.path.dirname(save_fn)):
      os.makedirs(os.path.dirname(save_fn))
    graph.save(save_fn, format=out_format)

  print "Total time for graph %.3f" % (time()-begin)
  return graph, save_fn

def main():
  parser = argparse.ArgumentParser(description="Create create time series graph(s) from a table in the database.")
  parser.add_argument("--tb_name", "-t", action="store", help="The table name containing the graph.")
  parser.add_argument("--db_name", "-d", default="Pydb", action="store", help="The database name containing the graph. Default is 'Pydb'.")
  parser.add_argument("--db_host", "-H", action="store", default="localhost", help="The database hostname/network address. Default is localhost.")
  parser.add_argument("--db_user", "-u", action="store", default="python", help="The name of the database user who will be reponsible for all transactions. Default is 'python'.")

  parser.add_argument("--save_dir", "-S", action="store", default="./graph_slices", help="Directory where you want the graph to save. Default is './graph_slices'.")
  parser.add_argument("--num_slices", "-n", action="store", type=int, default=1, help="The number of slices you want. Defualt is 1.")
  parser.add_argument("--continuous", "-c", action="store_true", help="If you want each slice to begin from the 'beginning of time'. \
                        If you don't select this all slices will not overlap in time.")


  parser.add_argument("--time_col","-tc", action="store", help="Column/Attribute name of the column containing the time attribute on which slicing will occur.")
  parser.add_argument("--src_col", "-sc", action="store", help="Column/Attribute name containing source vertices.")
  parser.add_argument("--dest_col", "-dc", action="store", help="Column/Attribute name containing destination vertices.")
  parser.add_argument("--weight_col", "-w", action="store", default="", help="Column/Attribute name containing edge weights. Default is ''.")
  parser.add_argument("--aggregation", "-a", action="store", default="sum", help="The type of operation to be performed to numerical columns in the slice. \
                        Default is sum. Options: 'sum', 'prod', 'avg', 'stddev', 'count', 'count(distinct)', 'max', 'min', 'variance'.")

  parser.add_argument("--out_format", "-o", action="store", help="""The format of the graphs you want to extract. Default is csv:
                      Choices are:
                      "csv": comma separated value edge list,
                      "tsv": tab separated value edge list,
                      "edgelist": numeric edge list format (space separated value edgelist),

                      "dot", "graphviz": GraphViz DOT format,
                      "gml": GML format,
                      "graphml" and "graphmlz": standard and gzipped GraphML format,
                      "gw", "leda", "lgr": LEDA native format,
                      "lgl": LGL format,
                      "ncol": NCOL format,
                      "net", "pajek": Pajek format,
                      "pickle", "picklez": standard and gzipped Python pickled format,
                      "svg": SVG format.*Note: Memory intensive.
                          """)
  parser.add_argument("--no_pass", "-np", action="store_true", help="Pass the flag if your user account has no password.")

  result = parser.parse_args()

  if not (result.tb_name and result.time_col and result.db_name and result.save_dir and result.src_col and result.dest_col and result.out_format):
    sys.stderr.write("You must at least define the following flags to run a cmd line job: '-T', '-t', '-d', '-S', '-sc', '-dc', '-o'\n")

  if result.no_pass:
    result.db_pass = ""

  else:
    result.db_pass = getpass("Please enter the '%s' user password for MySQL:" % result.db_user)

  authargs = {"db_host": result.db_host, "db_pass": result.db_pass, "db_name": result.db_name, "db_user":result.db_user}

  create_graph(result.tb_name, result.time_col, result.save_dir, result.src_col, result.dest_col, result.weight_col, result.out_format, result.num_slices, result.aggregation, **authargs)

if __name__ == "__main__":
  main()
