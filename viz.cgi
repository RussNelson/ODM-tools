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
varquery = """select SiteID, VariableID, VariableName, SampleMedium from seriescatalog where SiteID = %s order by VariableName;"""
graphquery = """select VariableName, VariableUnitsName from seriescatalog where SiteID = %s and VariableID = %s;"""
valuesquery = """select DateTimeUTC, DataValue from datavalues where SiteID = %s and VariableID = %s %s %s;"""

#| SeriesID | SiteID | SiteCode   | SiteName    | SiteType | VariableID | VariableCode | VariableName | Speciation     | VariableUnitsID | VariableUnitsName | SampleMedium | ValueType         | TimeSupport | TimeUnitsID | TimeUnitsName | DataType   | GeneralCategory | MethodID | MethodDescription  | SourceID | Organization        | SourceDescription                                                                                                                              | Citation | QualityControlLevelID | QualityControlLevelCode | BeginDateTime       | EndDateTime         | BeginDateTimeUTC    | EndDateTimeUTC      | ValueCount |

#|      222 |     34 | DutchApple | Dutch Apple | NULL     |        249 | windspeed    | Wind speed   | Not Applicable |             119 | meters per second | Air          | Field Observation |           0 |         100 | NULL          | Continuous | Instrumentation |        1 | Autonomous Sensing |        5 | Clarkson University | Clarkson University in conjunction with the Beacon Institute for Rivers and Estuaries conduct watershed wide monitoring in a real-time fashion | NULL     |                     0 | 0                       | 2012-10-14 13:15:00 | 2013-08-09 17:59:59 | 2012-10-14 18:15:00 | 2013-08-09 22:59:59 |      79900 |


sqlfind = """select sites.SiteID, variables.VariableID, methods.MethodID
             from sites, variables, methods
             where sites.SiteCode = '%s'
             and variables.VariableCode = '%s'
             and methods.MethodDescription = 'Autonomous Sensing'"""

begin, dateform, openlist, middle, graph, closelist, closeform, end = """<html><head>
<meta http-equiv="X-UA-Compatible" content="IE=EmulateIE7; IE=EmulateIE9"> 
<!--[if IE]><script src="/excanvas_r3/excanvas.js"></script><![endif]-->
   <meta charset="utf-8" />
   <meta http-equiv="content-type" content="text/html; charset=iso-8859-1"/>
   <link rel="stylesheet" href="http://code.jquery.com/ui/1.10.3/themes/smoothness/jquery-ui.css" />
   <script src="http://code.jquery.com/jquery-1.9.1.js"></script>
   <script src="http://code.jquery.com/ui/1.10.3/jquery-ui.js"></script>
   <link rel="stylesheet" href="/resources/demos/style.css" />
    <script type="text/javascript" src="/dygraph-combined.js"></script>

    <link rel="stylesheet" href="jquery.treeview/jquery.treeview.css" />
    <link rel="stylesheet" href="jquery.treeview/red-treeview.css" />
    <link rel="stylesheet" href="jquery.treeview/screen.css" />

    <script type="text/javascript" src="http://ajax.googleapis.com/ajax/libs/jquery/1.2.6/jquery.min.js"></script>
    <script src="jquery.treeview/lib/jquery.cookie.js" type="text/javascript"></script>
    <script src="jquery.treeview/jquery.treeview.js" type="text/javascript"></script>
    <script src="jquery.treeview/jquery.treeview.async.js" type="text/javascript"></script>

    <script type="text/javascript">
    $(document).ready(function(){
        $("#black").treeview({
            url: "http://ra-tes.org/viz2.cgi"
        })
    });
    </script>
   <title>Viz page</title></head><body>
    <ul id="black">
    </ul>
|<form submit="">
<label for="from">From</label>
<input type="text" id="from" name="from" value="%s"/>
<label for="to">to</label>
<input type="text" id="to" name="to" value="%s"/>
|<ul>
|<li>%s</li>
|<div id="rths" style="width:500px;"></div>
<script type="text/javascript">
  g = new Dygraph(
      document.getElementById("rths"),
      "%s",
      { 
       %s
       width: "520",
       title: "%s",
       ylabel: "%s",
      }
  );
</script>
You can also download this data in <a href="%s">CSV</a> format. All times UTC.
|</ul>
|<br><input type="submit" />
</form>
|</body>
</html>
""".split("|")

def iso8821(d):
    fields = d.split("/")
    if len(fields[2]) < 2:
        fields[2] = "20" + fields[2]
    return fields[2] + "-" + fields[0] + "-" + fields[1]

def getSiteName(cur, SiteID):
    cur.execute("Select SiteName from sites where SiteID = %s" % SiteID)
    return cur.fetchone()[0]

source = """
[
	{
		"text": "1. Pre Lunch (120 min)",
		"expanded": true,
		"classes": "important",
		"children":
		[
			{
				"text": "1.1 The State of the Powerdome (30 min)"
			},
		 	{
				"text": "1.2 The Future of jQuery (30 min)"
			},
		 	{
				"text": "1.2 jQuery UI - A step to richnessy (60 min)"
			}
		]
	},
	{
		"text": "2. Lunch  (60 min)"
	},
	{
		"text": "3. After Lunch  (120+ min)",
		"children":
		[
			{
				"text": "3.1 jQuery Calendar Success Story (20 min)"
			},
		 	{
				"text": "3.2 jQuery and Ruby Web Frameworks (20 min)"
			},
		 	{
				"text": "3.3 Hey, I Can Do That! (20 min)"
			},
		 	{
				"text": "3.4 Taconite and Form (20 min)"
			},
		 	{
				"text": "3.5 Server-side JavaScript with jQuery and AOLserver (20 min)"
			},
		 	{
				"text": "3.6 The Onion: How to add features without adding features (20 min)",
				"id": "36",
				"hasChildren": true
			},
		 	{
				"text": "3.7 Visualizations with JavaScript and Canvas (20 min)"
			},
		 	{
				"text": "3.8 ActiveDOM (20 min)"
			},
		 	{
				"text": "3.8 Growing jQuery (20 min)"
			}
		]
	}
]
"""

source = """
[
	{
		"text": "1. Pre Lunch (120 min)",
		"expanded": true,
		"classes": "important",
		"children":
		[
			{
				"text": "1.1 The State of the Powerdome (30 min)"
			},
		 	{
				"text": "1.2 The Future of jQuery (30 min)"
			},
		 	{
				"text": "1.2 jQuery UI - A step to richnessy (60 min)"
			}
		]
	},
	{
	},
	{
            "text": "3.6 The Onion: How to add features without adding features (20 min)",
            "id": "36",
            "hasChildren": true
        },
        {
		"text": "3. After Lunch  (120+ min)",
	}
]
"""

def main():

    con = MySQLdb.connect(host='127.0.0.1', user='odbinsert', passwd='bn8V9!rL', db='odm', port=3307)
    cur = con.cursor()

    form = cgi.FieldStorage(keep_blank_values = True)
    if "root" in form:
        print "Content-Type: application/json\n"
        if form['root'].value == 'source':
            # We return a list of dicts with tags understood by treeview
            cur.execute(sitequery)
            if True:
                print "["
                for row in cur.fetchall():
                    print '  {\n"hasChildren": true,\n"id":"%s",\n"text":"%s"\n},' % row
                print "]"
            else:
                print source
        elif len(form['root'].value.split("-")) == 2:
            print '[{"text":"',
            (siteid,variableid) = form['root'].value.split("-")
            cur.execute(graphquery % (siteid, variableid))
            (title, ylabel) = cur.fetchone()
            url = "http://ra-tes.org/viz2.cgi?siteid=%s&variableid=%s&graph=%s" % (
              siteid,
              variableid,
              title)
            options = ""
            print (graph % (url, options, title, ylabel, url)).replace('"', '\\"').replace('\n', '\\n'),
            print '"}]'
        else:
            cur.execute(varquery % form["root"].value)
            print "["
            for row in cur.fetchall():
                print '  {\n"hasChildren": true,\n"id":"%s-%s",\n"text":"%s (%s)"\n},' % row
            print "]"
             
    elif "siteid" in form and "variableid" not in form and "graph" not in form:

        print "Content-Type: text/html\n"
        print begin
        print "Site:", getSiteName(cur, form['siteid'].value)
        print dateform % (form["from"].value, form["to"].value)
        print """<input type="hidden" name="siteid" value="%s"/>""" % form["siteid"].value
        cur.execute(varquery % form["siteid"].value)
        for row in cur.fetchall():
            print '<br><input type="radio" name="variableid" value="%s">%s (%s)</a>' % row
        print closeform
        print end

    elif "siteid" in form and "variableid" in form and "graph" not in form:
        cur.execute(graphquery % (form["siteid"].value, form["variableid"].value))
        (title, ylabel) = cur.fetchone()
        url = "?siteid=%s&variableid=%s&from=%s&to=%s&graph=%s" % (
          form["siteid"].value,
          form["variableid"].value,
          form["from"].value,
          form["to"].value,
          title)
        options = ""
        print "Content-Type: text/html\n"
        print begin
        print "Site:", getSiteName(cur, form['siteid'].value)
        print graph % (url, url, options, title, ylabel)
        print end
         
    elif "siteid" in form and "variableid" in form and "graph" in form:
        fromdate = form["from"].value
        todate = form["to"].value
        if fromdate: fromdate = " and DateTimeUTC >= '%s'" % iso8821(fromdate)
        if   todate:   todate = " and DateTimeUTC <= '%s'" % iso8821(todate)
        cur.execute(valuesquery % (form["siteid"].value, form["variableid"].value, fromdate, todate))
        print "Content-Type: text/plain\n"
        print "Date,%s" % form["graph"].value
        for row in cur.fetchall():
            print "%s,%s" % row

    else:
        cur.execute(sitequery)
        print "Content-Type: text/html\n"
        print begin
        print dateform % ("", "")
        for row in cur.fetchall():
            print '<br><input type="radio" name="siteid" value="%s">%s</a>' % row
        print closeform
        print end

main()

"""SELECT col_1, date_col, col_3 FROM tbl
WHERE
   date_col = ( SELECT min(date_col) FROM tbl
   WHERE
       year(date_col) = 2006 AND
       month(date_col) = 02
);"""

