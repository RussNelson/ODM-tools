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
valuesquery = """select DateTimeUTC, DataValue from datavalues where SiteID = %s and VariableID = %s %s %s;"""

sqlfind = """select sites.SiteID, variables.VariableID, methods.MethodID
             from sites, variables, methods
             where sites.SiteCode = '%s'
             and variables.VariableCode = '%s'
             and methods.MethodDescription = 'Autonomous Sensing'"""

# read the HTML template into m. Template has HTML comments where it gets split.
# the content of the all-lowercase comment is the name of the section.
model = open("viz.html").read()
sname = 'begin'
m = {}
for s in re.split(r'<!--([a-z]+?)-->', model):
    if sname is not None:
        m[sname] = s
        sname = None
    else:
        sname = s

def iso8821(d):
    fields = d.split("/")
    if len(fields[2]) < 2:
        fields[2] = "20" + fields[2]
    return fields[2] + "-" + fields[0] + "-" + fields[1]

def getSiteInfo(cur, SiteID):
    cur.execute("Select SiteName, Latitude, Longitude, County from sites where SiteID = %s" % SiteID)
    (site, lat, lon, county) = cur.fetchone()
    return "<p>Site: %s</p>\n" % site + m['leaftop'] % ("100", "[%s,%s], 13" % (lat, lon)) + m['leafpin'] % (lat, lon, county) + m['leafbottom'] + "<p>County: %s</p>" % county

def main():

    con = MySQLdb.connect(host='127.0.0.1', user='odbinsert', passwd='bn8V9!rL', db='odm', port=3307)
    cur = con.cursor()

    form = cgi.FieldStorage(keep_blank_values = True)
    if ("siteid" in form or "sitename" in form) and "variableid" not in form and "graph" not in form:
        if "siteid" in form:
            siteid = form['siteid'].value
        else:
            cur.execute("select SiteID from sites where SiteName = '%s'" % form["sitename"].value)
            siteid = cur.fetchone()[0]

        print "Content-Type: text/html\n"
        print m['begin']
        print getSiteInfo(cur, siteid)
        if "from" in form:
            fromval = form["from"].value
        else:
            fromval = ""
        if "to" in form:
            toval = form["to"].value
        else:
            toval = ""
        print m['dateform'] % (fromval, toval)
        print """<input type="hidden" name="siteid" value="%s"/>""" % siteid
        cur.execute(varquery % siteid)
        for row in cur.fetchall():
            print '<br><input type="radio" name="variableid" value="%s">%s (%s)</a>' % row
        print m['closeform']
        print m['end']

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
        print m['begin']
        print getSiteInfo(cur, form['siteid'].value)
        print m['graph'] % (url, url, options, title, ylabel)
        print m['end']
         
    elif "siteid" in form and "variableid" in form and "graph" in form:
        # note: this snippet returns CSV, not HTML
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
        print m['begin']
        print m['dateform'] % ("", "")
        for row in cur.fetchall():
            print '<br><input type="radio" name="siteid" value="%s">%s</a>' % row
        print m['closeform']
        print m['end']

main()

"""SELECT col_1, date_col, col_3 FROM tbl
WHERE
   date_col = ( SELECT min(date_col) FROM tbl
   WHERE
       year(date_col) = 2006 AND
       month(date_col) = 02
);"""

