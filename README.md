mysql_igraph
============

'igraph' extension that uses MySQL to store and create time series graphs on the fly


MySQL Setup for extension
=========================

You can TODO...

If you already have a mysql user account that you want to use omit step 1

1. Login to mysql as root => mysql -u root -p

Create a 'python' user within MySQL if one does not already exist. No password is preferable =>
CREATE USER 'python'@'localhost' IDENTIFIED BY '';

Create database you would like to use store your data => CREATE DATABASE Pydb;

Grant access to your database (and any others you like) to 'python' user =>
GRANT ALL ON Pydb.* TO 'python'@'localhost';

That's it!

Installation
============

This is full source simply download the package to any location. If all dependencies are present all should go well!

Dependencies
============

- python 2.7.
- python-mysq.
- igraph +0.6.
- MySQL-client MySQL-server (Not compatible with 5.4. Tested compatible with 5.1-5.3, 5.5).


Examples of use:
================

Ingest example:
---------------
Suppose we have an input data file named data.tsv. The following:

python ingest.py data.tsv "\t" -i 0
, would insert the data.tsv file into the database. The second argument is the type of separator used in the file -- here we specified "\t" (tab). The last argument ("-i") is optional and sets which (if any) column indexes of the input file to ignore and not insert into the DB.

Notice that all tsv input headers are turned into attribute names and types are deduced automatically based on the 1st row of data. This is default behavior that is alterable via other flags which can be displayed by using the help flag as follows:

python ingest.py -h


Slicing (partition for time series graphs) example:
--------------------------------------------------
Here is how one might call the script:

python slice.py -T table_name -t time_attr_name -d Pydb -S save_result_path -n no_of_graphs -sc source_attr_name -dc dest_attr_name -w weight_attr_name -o output_format
This will produce 'no_of_graphs' graphs in your selected output format as described above. All flags are described in detail if you run:

python slice.py -h
In this example:

table_name – is the name of table which you intend to use to generate the time series graphs e.g myTable
time_attr_name – is the name of column that contains the time attribute on which the graph will be partitioned e.g timecol1
save_result_path – the path to where you want the graphs written on disk e.g /home/myname/Documents/data
no_of_graphs – the integer number of time series graphs you desire. e.g 1
source_attr_name -- the name of the field that contains the source node IDs. e.g srccol1
dest_attr_name --  the name of the field that contains the destination node IDs. e.g destcol1
weight_attr_name -- the name of the field that contains the weight attribute. e.g wightcol1.
Note that many different kinds of aggregation can be performed to the weight attribute prior to graph creation by passing the -a flag.
output_format -- your choice of graph format. Supported types noted above. E.g dot


Interactive data exploration mode:
---------------------------------
It's tough to remember what data is in which table and/or what attribute data types/name are. To address this there is an interactive driver that allows one to inspect tables in the database. The alternative is obviously to run the MySQL command line tool and inspect the tables using sql queries yourself, which is much more work and tough for SQL novices.

Interactive mode example use:
If we want to inspect the 'Pydb' database. This is accomplished as follows:

python slice.py -i -d Pydb
This assumes the 'Pydb' database already exists and the 'python' user has permission to it.

Supported formats
=================

Input:
------
The extension is currently implemented to ingest only '*' separated value formats e.g csv (comma), tsv (tab), ssv (space) etc

Output:
------
The resulting graphs can be saved in the following formats:

"csv": comma separated value edge list
"tsv": tab separated value edge list
"edgelist": numeric edge list format (space separated value edgelist)
"dot", "graphviz": GraphViz DOT format
"gml": GML format
"graphml" and "graphmlz": standard and gzipped GraphML format
"gw", "leda", "lgr": LEDA native format
"lgl": LGL format
"ncol": NCOL format
"net", "pajek": Pajek format
"pickle", "picklez": standard and gzipped Python pickled format
"svg": SVG format

*All may be weighted or unweighted

Ingest benchmarks (Data input rate):
Benchmarked to perform at a minimum of 0.816(± 0.072) µs/line. Ingesting 100 Million lines of a tab separated file .tsv in 90.3s (1.5 min).
Benchmark performed on an 8-core, 2.4 Ghz, 32GB server.
The rate of ingest can be increased by *permitting the 'python' process the authority to pass the following commands "FLUSH tables", "myisamchk"*.

Time formats accepted are the sql types datetime, date, time, year, timestamp as documented by mysql http://dev.mysql.com/doc/refman/5.5/en/date-and-time-types.html.

Sample Data:
===========
A few very short sample data files are included in the sampledata folder.

Need help?
==========
The *action* scripts (currently ingest.py, slice.py) can be passed -h flag for help. Help there is detailed and exhaustive.
Other questions can be emailed to disa@jhu.edu.