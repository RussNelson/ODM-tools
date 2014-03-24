#!/usr/bin/python
# vim: set ai sw=4 sta fo=croql ts=8 expandtab syntax=python
                                                                                # die, PEP8's 80-column punched card requirement!

# https://github.com/nicekei/jQuery-html5-canvas-panorama-plugin

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

blacklist = map(int, "260 261 262".split())

sitequery = """select SiteID, SiteName  from sites order by SiteName;"""
varquery = """select VariableID, VariableName, SampleMedium, MethodDescription from seriescatalog where SiteID = %s order by VariableName;"""
varsquery = """select distinct VariableID, VariableName, SampleMedium, MethodDescription from seriescatalog order by VariableName;"""
graphquery = """select BeginDateTimeUTC, EndDateTimeUTC, VariableName, VariableUnitsName, SampleMedium, MethodDescription
                from seriescatalog where SiteID = %s and VariableID = %s;"""
timequery = """select min(BeginDateTimeUTC), max(EndDateTimeUTC) from seriescatalog where SiteID = %s;"""
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

def iso8601(d):
    fields = d.split("/")
    if len(fields) == 1:
        return d
    if len(fields[2]) < 2:
        fields[2] = "20" + fields[2]
    return fields[2] + "-" + fields[0] + "-" + fields[1]

def getSiteInfo(cur, SiteID):
    cur.execute("Select SiteCode, SiteName, Latitude, Longitude, County from sites where SiteID = %s" % SiteID)
    (sitecode, sitename, lat, lon, county) = cur.fetchone()
    html = ("<p>Site: %s County: %s</p>\n" % (sitename, county)  +
             m['acmemapper'] % (lat, lon) +
             m['leaftop'] % ("100", "[%s,%s], 13" % (lat, lon)) +
             m['leafpin'] % (lat, lon, county) +
             m['leafbottom']
           )
    fn = "panoramas/%s.jpg" % sitecode
    if os.path.exists(fn):
        html += m['panorama'] % (sitecode, fn, sitecode)
    return sitename, html

def full_name(sample, method):
    title = ''
    if sample != 'Not Relevant':
        title += " in " + sample
    if method != 'Autonomous Sensing':
        title += " using " + method
    return title 

def parse_date(d, epoch):
    if d:
        d = d.split()[0]
    if "/" in d:
        dt = datetime.datetime(*time.strptime(d, "%m/%d/%Y")[:6])
    elif "-" in d:
        dt = datetime.datetime(*time.strptime(d, "%Y-%m-%d")[:6])
    elif epoch:
        dt = datetime.datetime(datetime.MINYEAR, 1, 1)
    else:
        dt = datetime.datetime(datetime.MAXYEAR, 1, 1)
    return dt

def simpleiso(d):
    return d.isoformat().split("T")[0]

def printgraph( cur, siteid, variableid, rthsno, fromdate, todate, location=None):
    cur.execute(graphquery % (siteid, variableid))
    row = cur.fetchone()
    if row is None:
        print "<hr>We have no data"
        if location is not None:
            print " at ",location
        return
    (begindate, enddate, name, ylabel, sample, method) = row
    title = name + full_name(sample, method)
    if location is not None:
        title += " at " + location

    if variableid == '231': # ppal or ppal1
        options = "stepPlot: true,"
    elif variableid == '250': # wind direction
        options = 'drawPoints: true,\nstrokeWidth: 0,'
    else:
        options = ""

    fromdatedt = parse_date(fromdate, False)
    todatedt = parse_date(todate, True)

    if fromdatedt > enddate or todatedt < begindate:
        print "<hr>We have no data for",title,"from",fromdate," to ",todate
    else:
        url = "?state=graphcsv&siteid=%s&variableid=%s&from=%s&to=%s&title=%s" % (
          siteid,
          variableid,
          fromdate, 
          todate,
          title)
        print m['graph'] % (rthsno, url, url+"&excel=yes", rthsno, url, options, title, ylabel)

def main():

    con = MySQLdb.connect(host='127.0.0.1', user='root', passwd='drjim1979', db='odm', port=3306)
    cur = con.cursor()

    form = cgi.FieldStorage(keep_blank_values = True)

    if len(form) == 0 and os.environ['QUERY_STRING']:
        sitename = os.environ['QUERY_STRING'].replace("%20"," ")
        state = "site"
    elif "state" not in form:
        state = "sitelist"
    else:
        state = form["state"].value

    if state == "sitelist":
        print "Content-Type: text/html\n"
        print m['begin'] % "Sitelist"
        print m['dateform'] % (
            simpleiso((datetime.datetime.now() - datetime.timedelta(days=14))),
            simpleiso(datetime.datetime.now())
            )
        print """<input type="hidden" name="state" value="site"/>"""
        cur.execute(sitequery)
        for siteid, sitename in cur.fetchall():
            print '<br><input type="radio" id="%s" name="siteid" value="%s"/><label for="%s">%s</label>' % (siteid, siteid, siteid, sitename)
        print m['closeform']
        print m['end']
    elif state == "panos":
        print "Content-Type: text/html\n"
        print m['begin'] % "All of our panoramas"
        for root, dirs, files in os.walk('panoramas'):
            del dirs[:] # don't recurse
            for name in files:
                fn = "panoramas/%s" % name
                id = os.path.splitext(name)[0]
                print m['panorama'] % (id, fn, id)
        print m['end']
    elif state == "site":
        # we get called with either siteid or sitename.
        if "siteid" in form:
            siteid = form['siteid'].value
        else:
            if "sitename" in form:
                sitename = form["sitename"].value
            cur.execute("select SiteID from sites where SiteName = '%s'" % sitename)
            results = cur.fetchone()
            if results:
                siteid = results[0]
            else:
                print "Content-Type: text/html\n"
                print m['begin'] % "Error"
                print "No site named '%s' found, sorry" % sitename
                print m['end']
                return

        (sitename, sitehtml) = getSiteInfo(cur, siteid)

        print "Content-Type: text/html\n"
        print m['begin'] % sitename
        print sitehtml

        cur.execute(timequery % (siteid))
        (begintime, endtime) = cur.fetchone()

        if "from" in form and form["from"].value:
            fromval = form["from"].value
        else:
            fromval = begintime
        if "to" in form and form["to"].value:
            toval = form["to"].value
        else:
            toval = endtime
        print m['dateform'] % (fromval, toval)
        print """<input type="hidden" name="state" value="variables"/>"""
        print """<input type="hidden" name="siteid" value="%s"/>""" % siteid
        print m['checkall']
        cur.execute(varquery % siteid)
        for (variableid, variablename, samplemedium, methoddescription) in cur.fetchall():
            if variableid not in blacklist:
                variablename += full_name(samplemedium, methoddescription)
                print m['checkboxes'] % (variableid, variablename)
        print m['closechecks']
        print m['closeform']
        print m['end']

    elif state == "crosslist":
        print "Content-Type: text/html\n"
        print m['begin'] % "Cross-site variables"
        print m['dateform'] % (
            simpleiso((datetime.datetime.now() - datetime.timedelta(days=14))),
            simpleiso(datetime.datetime.now())
            )
        print """<input type="hidden" name="state" value="cross"/>"""
        cur.execute(varsquery)
        for (variableid, variablename, samplemedium, methoddescription) in cur.fetchall():
            if variableid not in blacklist:
                variablename += full_name(samplemedium, methoddescription)
                print '<br><input type="radio" id="%s" name="variableid" value="%s"/><label for="%s">%s</label>' % (variableid, variableid, variableid, variablename)
        print m['closeform']
        print m['end']
            
    elif state == "cross":
        variableid = form['variableid'].value
        fromdate = form["from"].value
        todate = form["to"].value
        print "Content-Type: text/html\n"
        print m['begin'] % "Cross-site graphs"
        cur.execute(sitequery)
        rthsno = 0
        for siteid, sitename in cur.fetchall():
            printgraph( cur, siteid, variableid, rthsno, fromdate, todate, sitename)
            rthsno += 1
        print m['end']

    elif state == "variables":
        # we get called with the site id and one or more variableid's.
        siteid = form['siteid'].value
        (sitename, sitehtml) = getSiteInfo(cur, siteid)

        print "Content-Type: text/html\n"
        print m['begin'] % ("Variables at " + sitename)
        print sitehtml

        rthsno = 0
        for variables in form.keys():
            fields = variables.split("-")
            if len(fields) < 2: continue
            variableid = fields[1]
            fromdate = form["from"].value
            todate = form["to"].value
            printgraph( cur, siteid, variableid, rthsno, fromdate, todate)
            rthsno += 1
        print m['end']
         
    elif state == "graphcsv":
        # note: this snippet returns CSV, not HTML
        fromdate = form["from"].value
        todate = form["to"].value
        excel = 'excel' in form
        if fromdate: fromdate = " and DateTimeUTC >= '%s'" % iso8601(fromdate)
        if   todate:   todate = " and DateTimeUTC <= '%s'" % iso8601(todate)
        cur.execute(valuesquery % (form["siteid"].value, form["variableid"].value, fromdate, todate))
        print "Content-Type: text/plain\n"
        print 'UTC Date,"%s"' % form["title"].value
        for row in cur.fetchall():
            (dt, value) = row
            if not excel:
                dt = (str(dt).replace(" ", "T") + "Z")
            print "%s,%s" % (dt, value)

    else:
        print "Content-Type: text/html\n"
        print "Well, THAT broke quickly."

main()

"""SELECT col_1, date_col, col_3 FROM tbl
WHERE
   date_col = ( SELECT min(date_col) FROM tbl
   WHERE
       year(date_col) = 2006 AND
       month(date_col) = 02
);"""

