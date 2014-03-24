#!/usr/bin/python
# vim: set ai sw=4 sta fo=croql ts=8 expandtab syntax=python
                                                                                # die, PEP8's 80-column punched card requirement!

import time
import sys
import gzip
import os
import csv
import re
import fnmatch
import MySQLdb
from all import rths_sites, rths_sensors
import datetime
import math
import cgi

sitequery = """select SiteID, SiteName  from sites order by SiteName;"""
varquery = """select VariableID, VariableName, SampleMedium from seriescatalog where SiteID = %s order by VariableName;"""
graphquery = """select VariableName, VariableUnitsName from seriescatalog where SiteID = %s and VariableID = %s;"""
valuesquery = """select DateTimeUTC, DataValue from datavalues where SiteID = %s and VariableID = %s;"""

#| SeriesID | SiteID | SiteCode   | SiteName    | SiteType | VariableID | VariableCode | VariableName | Speciation     | VariableUnitsID | VariableUnitsName | SampleMedium | ValueType         | TimeSupport | TimeUnitsID | TimeUnitsName | DataType   | GeneralCategory | MethodID | MethodDescription  | SourceID | Organization        | SourceDescription                                                                                                                              | Citation | QualityControlLevelID | QualityControlLevelCode | BeginDateTime       | EndDateTime         | BeginDateTimeUTC    | EndDateTimeUTC      | ValueCount |

#|      222 |     34 | DutchApple | Dutch Apple | NULL     |        249 | windspeed    | Wind speed   | Not Applicable |             119 | meters per second | Air          | Field Observation |           0 |         100 | NULL          | Continuous | Instrumentation |        1 | Autonomous Sensing |        5 | Clarkson University | Clarkson University in conjunction with the Beacon Institute for Rivers and Estuaries conduct watershed wide monitoring in a real-time fashion | NULL     |                     0 | 0                       | 2012-10-14 13:15:00 | 2013-08-09 17:59:59 | 2012-10-14 18:15:00 | 2013-08-09 22:59:59 |      79900 |


sqlfind = """select sites.SiteID, variables.VariableID, methods.MethodID
             from sites, variables, methods
             where sites.SiteCode = '%s'
             and variables.VariableCode = '%s'
             and methods.MethodDescription = 'Autonomous Sensing'"""

begin, openlist, middle, graph, closelist, end = """<html><head>
<meta http-equiv="X-UA-Compatible" content="IE=EmulateIE7; IE=EmulateIE9"> 
<!--[if IE]><script src="/excanvas_r3/excanvas.js"></script><![endif]-->
   <meta charset="utf-8" />
   <title>jQuery UI Datepicker - Default functionality</title>
   <link rel="stylesheet" href="http://code.jquery.com/ui/1.10.3/themes/smoothness/jquery-ui.css" />
   <script src="http://code.jquery.com/jquery-1.9.1.js"></script>
   <script src="http://code.jquery.com/ui/1.10.3/jquery-ui.js"></script>
   <link rel="stylesheet" href="/resources/demos/style.css" />
   <script>
   $(function() {
     $( "#datepicker" ).datepicker();
     $( "#datepicker1" ).datepicker();
     });
   </script>
  <script type="text/javascript" src="/dygraph-combined.js"></script>
  <title>Viz page</title></head><body>
|<ul>
|<li>%s</li>
|<div id="rths" style="width:500px;"></div>
You can also download this data in <a href="%s">CSV</a> format. All times UTC.
<script type="text/javascript">
  g = new Dygraph(
      document.getElementById("rths"),
      "%s",
      { 
       %s
       width: '520',
       title: '%s',
       ylabel: '%s',
      }
  );
</script>
|</ul>
|</body>
</html>
""".split("|")

def main():

    con = MySQLdb.connect(host='127.0.0.1', user='odbinsert', passwd='bn8V9!rL', db='odm', port=3307)
    cur = con.cursor()

    form = cgi.FieldStorage()
    if "siteid" in form and "variableid" not in form and "graph" not in form:
        cur.execute(varquery % form["siteid"].value)

        print "Content-Type: text/html\n"
        print begin
        print "Site:", form['sitename'].value
        print openlist
        listline = ('<a href="?siteid=SITEID&sitename=SITENAME&variableid=%s">%s (%s)</a>'
                     .replace("SITEID", form["siteid"].value)
                     .replace("SITENAME", form["sitename"].value))
        for row in cur.fetchall():
            print middle % (listline % row)
        print closelist
        print end

    elif "siteid" in form and "variableid" in form and "graph" not in form:
        cur.execute(graphquery % (form["siteid"].value, form["variableid"].value))
        (title, ylabel) = cur.fetchone()
        url = "?siteid=%s&variableid=%s&graph=%s" % (form["siteid"].value, form["variableid"].value, title)
        options = ""
        print "Content-Type: text/html\n"
        print begin
        print "Site:", form['sitename'].value
        print graph % (url, url, options, title, ylabel)
        print end
         
    elif "siteid" in form and "variableid" in form and "graph" in form:
        cur.execute(valuesquery % (form["siteid"].value, form["variableid"].value))
        print "Content-Type: text/plain\n"
        print "Date,%s" % form["graph"].value
        for row in cur.fetchall():
            print "%s,%s" % row

    else:
        cur.execute(sitequery)
        print "Content-Type: text/html\n"
        print begin
        print """Begin Date: <input type="text" id="datepicker" name="begindate" />"""
        print """End Date: <input type="text" id="datepicker1" name="enddate" />"""
        print openlist
        for row in cur.fetchall():
            r = list(row)
            r.append(row[1])
            print middle % ('<a href="?siteid=%s&sitename=%s">%s</a>' % tuple(r))
        print closelist
        print end

main()

"""SELECT col_1, date_col, col_3 FROM tbl
WHERE
   date_col = ( SELECT min(date_col) FROM tbl
   WHERE
       year(date_col) = 2006 AND
       month(date_col) = 02
);"""

