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
import string
import math
import cgi
import json
import types

blacklist = "bucketdepthdeep bucketdepth buckettemp boardtemperature cabinettemperature optical-green optical-blue reference".split()

sitequery = """select SiteID, SiteName  from sites order by SiteName;"""
varquery = """select SeriesID, VariableCode, VariableName, SampleMedium, MethodDescription from seriescatalog where SiteID = %s order by VariableName;"""
seriesquery = """select SeriesID, SiteID, SiteName, VariableCode, VariableName, SampleMedium, MethodDescription from seriescatalog 
                 where VariableID = %s order by SiteID;"""
varsquery = """select distinct VariableID, VariableCode, VariableName, SampleMedium, MethodDescription from seriescatalog order by VariableName;"""
graphquery = """select BeginDateTimeUTC, EndDateTimeUTC, VariableName, VariableUnitsName, VariableCode, SampleMedium, MethodDescription
                from seriescatalog where SeriesID = %s;"""
timequery = """select min(BeginDateTimeUTC), max(EndDateTimeUTC) from seriescatalog where SiteID = %s;"""
valuesquery = """select DateTimeUTC, DataValue from datavalues, seriescatalog where
                 datavalues.SiteID = seriescatalog.SiteID and
                 datavalues.VariableID = seriescatalog.VariableID and
                 datavalues.MethodID = seriescatalog.MethodID and
                 seriescatalog.SeriesID = %s""" # missing semicolon on purpose.

# Read the HTML template into m. It contains multiple named sections separated
# by HTML comments. The content of the HTML comment is the name of the section
# that follows. Since the first section has no comment, it is named 'begin'
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

# http://code.activestate.com/recipes/59857-filter-a-string-and-only-keep-a-given-set-of-chara/
def makefilter(keep):
    """ Return a functor that takes a string and returns a copy of that
        string consisting of only the characters in 'keep'.
    """

    # make a string of all chars, and one of all those NOT in 'keep'
    allchars = string.maketrans('', '')
    delchars = ''.join([c for c in allchars if c not in keep])

    # return the functor
    return lambda s,a=allchars,d=delchars: s.translate(a, d)

identifier = makefilter(string.letters + "." + string.digits)
date_chars_only = makefilter("-: " + string.digits)

def getSiteInfo(cur, SiteID):
    cur.execute("Select SiteCode, SiteName, Latitude, Longitude, County from sites where SiteID = %s" , SiteID)
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

def datetimeToGSON(dt):
    return "'Date(" + ",".join([str(i) for i in dt.timetuple()[0:6]]) + ")'"

def printgraph( cur, configfn, siteid, seriesid, rthsno, fromdate, todate, location=None):
    cur.execute(graphquery, seriesid)
    row = cur.fetchone()
    if row is None:
        print "<hr>We have no data"
        if location is not None:
            print " at ",location
        return
    (begindate, enddate, name, ylabel, variablecode, sample, method) = row
    title = name + full_name(sample, method)
    if location is not None:
        title += " at " + location

    # FIXME force http://dygraphs.com/options.html#valueRange for water temperatures that include freezing.
    if variablecode == 'precip':
        options = "stepPlot: true,"
    elif variablecode == 'winddir':
        options = 'drawPoints: true,\nstrokeWidth: 0,'
    else:
        options = ""
    options += "title: '%s', ylabel: '%s'," % (title, ylabel)

    fromdatedt = parse_date(fromdate, False)
    todatedt = parse_date(todate, True)

    if fromdatedt > enddate or todatedt < begindate:
        print "<hr>We have no data for",title,"from",fromdate," to ",todate
    else:
        url = "?config=%s&state=graphcsv&siteid=%s&seriesid=%s&from=%s&to=%s&title=%s" % (
          configfn,
          siteid,
          seriesid,
          fromdate, 
          todate,
          title)
        print m['graph'] % (rthsno, url, url+"&excel=yes", rthsno, url, options)
    return title

def main():

    form = cgi.FieldStorage(keep_blank_values = True)

    if "state" not in form and len(form) == 1:
        sitename = form.keys()[0].replace("%20"," ")
        state = "site"
    elif "state" not in form:
        state = "sitelist"
    else:
        state = form["state"].value

    if "config" not in form:
        configfn = "config.json"
    else:
        configfn = form["config"].value.translate(None, '"/%&\\<>{}[]')

    config = json.load(open(configfn))
    con = MySQLdb.connect(**config)
    cur = con.cursor()

    if state == "sitelist":
        print "Content-Type: text/html\n"
        print m['begin'] % "Sitelist"
        print m['dateform'] % (
            simpleiso((datetime.datetime.now() - datetime.timedelta(days=14))),
            simpleiso(datetime.datetime.now())
            )
        print """<input type="hidden" name="state" value="site"/>"""
        print """<input type="hidden" name="config" value="%s"/>""" % configfn
        cur.execute(sitequery)
        for siteid, sitename in cur.fetchall():
            print '<br><input type="radio" id="%s" name="siteid" value="%s"/><label for="%s">%s</label>' % (siteid, siteid, siteid, sitename)
        print m['closeform']
        print m['adcp']
        print m['end']
    elif state == "panos":
        print "Content-Type: text/html\n"
        print m['begin'] % "All of our panoramas"
        names = []
        for root, dirs, files in os.walk('panoramas'):
            del dirs[:] # don't recurse
            names += files
        names.sort()
        for name in names:
            fn = "panoramas/%s" % name
            id = os.path.splitext(name)[0]
            print '<div class="span-1">'
            print '<div class="rotate"><a href="viz.cgi?%s">%s</a></div>' % (name.replace(".jpg",""), name.replace(".jpg",""))
            print '</div>'
            print m['panorama'] % (id, fn, id)
        print m['end']
    elif state == "site":
        # we get called with either siteid or sitename.
        everything = 'everything' in form
        if "siteid" in form:
            siteid = identifier(form['siteid'].value)
        else:
            if "sitename" in form:
                sitename = identifier(form["sitename"].value)
            cur.execute("select SiteID from sites where SiteCode = %s", sitename.replace(" ",""))
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

        cur.execute(timequery, siteid)
        (begintime, endtime) = cur.fetchone()

        if "from" in form and form["from"].value:
            fromval = date_chars_only(iso8601(form["from"].value))
        else:
            fromval = begintime
        if "to" in form and form["to"].value:
            toval = date_chars_only(iso8601(form["to"].value))
        else:
            toval = endtime
        print m['dateform'] % (fromval, toval)
        print """<input type="hidden" name="state" value="serieses"/>"""
        print """<input type="hidden" name="siteid" value="%s"/>""" % siteid
        print """<input type="hidden" name="config" value="%s"/>""" % configfn
        print m['checkall']
        cur.execute(varquery, siteid)
        sortorder = 0
        for (seriesid, variablecode, variablename, samplemedium, methoddescription) in cur.fetchall():
            if everything or variablecode not in blacklist:
                vsm = variablename + full_name(samplemedium, methoddescription)
                print m['checkboxes'] % (seriesid, sortorder, vsm)
                sortorder += 1
        print m['closechecks']
        print m['closeform']
        print m['end']

    elif state == "crosslist":
        everything = 'everything' in form
        print "Content-Type: text/html\n"
        print m['begin'] % "Cross-site variables"
        print m['dateform'] % (
            simpleiso((datetime.datetime.now() - datetime.timedelta(days=14))),
            simpleiso(datetime.datetime.now())
            )
        print """<input type="hidden" name="state" value="cross"/>"""
        print """<input type="hidden" name="config" value="%s"/>""" % configfn
        cur.execute(varsquery)
        for (variableid, variablecode, variablename, samplemedium, methoddescription) in cur.fetchall():
            if everything or variablecode not in blacklist:
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
        cur.execute(seriesquery, variableid)
        rthsno = 0
        allsites = list(cur.fetchall())
        allsites.sort( lambda a,b: cmp(a[2], b[2]) )
        for (seriesid, siteid, sitename, variablecode, variablename, samplemedium, methoddescription) in allsites:
            printgraph( cur, configfn, siteid, seriesid, rthsno, fromdate, todate, sitename)
            rthsno += 1
        print m['end']

    elif state == "serieses":
        # we get called with the site id and one or more seriesid's.
        siteid = form['siteid'].value
        (sitename, sitehtml) = getSiteInfo(cur, siteid)

        print "Content-Type: text/html\n"
        print m['begin'] % ("Variables at " + sitename)
        print sitehtml

        rthsno = 0
        fromdate = form["from"].value
        todate = form["to"].value
        serieses = form.keys()
        serieses.sort( lambda a,b: cmp(form[a], form[b]) )
        seriesids = []
        for series in serieses:
            fields = series.split("-")
            if len(fields) < 2: continue
            seriesid = fields[1]
            seriesids.append( (seriesid, printgraph( cur, configfn, siteid, seriesid, rthsno, fromdate, todate)) )
            rthsno += 1
        if rthsno == 0:
            print "But you didn't check any variables!"
        series = [ "seriesid=%s&title=%s" % s for s in seriesids ]
        if rthsno == 2:
            url = "?state=graphcsv&config=%s&siteid=%s&from=%s&to=%s&%s" % (configfn, siteid, fromdate, todate, "&".join(series))
            options = 'connectSeparatedPoints:true,'
            options += "title: '%s and %s'," % (seriesids[0][1], seriesids[1][1])
            print m['graph'] % (rthsno, url, url+"&excel=yes", rthsno, url, options)
            rthsno += 1
            url += "&nodate=y"
            options = 'connectSeparatedPoints:true,'
            options += "title: '%s', ylabel: '%s'," % (seriesids[0][1], seriesids[1][1])
            print m['graph'] % (rthsno, url, url+"&excel=yes", rthsno, url, options)
            print "<script>graphlist.pop();</script>" # don't sync this one.
            rthsno += 1
        url = "?state=graphcsv&config=%s&siteid=%s&from=%s&to=%s&%s" % (configfn, siteid, fromdate, todate, "&".join(series))
        print """<hr>Everything in one <a href="%s">standard CSV</a> or <a href="%s">Excel CSV</a> file""" % (url, url+ "&excel=yes")
        print """<script type="text/javascript">var sync = Dygraph.synchronize(graphlist, { zoom: true, range: false, selection: true });</script>"""
        print m['end']

    elif state == "graphcsv":
        # note: this snippet returns CSV, not HTML
        fromdate = form["from"].value
        todate = form["to"].value
        excel = 'excel' in form
        nodate = 'nodate' in form
        vq = valuesquery
        fd = date_chars_only(iso8601(fromdate))
        if fd.find(" ") < 0:
            fd += "T00:00:00"
        else:
            fd = fd.replace(" ","T")
        td = date_chars_only(iso8601(todate))
        if td.find(" ") < 0:
            td += "T23:59:59"
        else:
            td = td.replace(" ","T")
        if fromdate: vq += " and DateTimeUTC >= '%s'" % fd
        if   todate: vq += " and DateTimeUTC <= '%s'" % td
        vq += ';'
        print "Content-Type: text/plain\n"
        if type(form["seriesid"]) == types.ListType:
            seriesids = form["seriesid"]
            titles = form["title"]
        else:
            seriesids = [form["seriesid"]]
            titles = [form["title"]]
        if not nodate:
            print 'UTC Date,',
        print ",".join([ '"%s"' % title.value.translate(None, '"%&\\<>{}[]').replace(",","") for title in titles])
        if fromdate and not nodate:
            if excel:
                fd = fd.replace("T"," ")
            else:
                fd = fd + "Z"
            print "%s%s" % (fd, "," * len(seriesids))
        pairs = []
        for i, series in enumerate(seriesids):
            cur.execute(vq, series.value)
            for row in cur.fetchall():
                (dt, value) = row
                dt += datetime.timedelta(seconds = 59) # round up to next minute (only needed for last measurement of the hour)
                dt -= datetime.timedelta(minutes = dt.minute % 5, seconds = dt.second) # round down to current multiple of 5 minutes.
                if not excel:
                    dt = (str(dt).replace(" ", "T") + "Z")
                pairs.append( (dt, value, i) )
        pairs.sort()
        thisdt = None
        for dt, value, i in pairs:
            if dt != thisdt:
                if thisdt is not None:
                    columns[i] = value
                    if not nodate or None not in columns:
                        columns = [ str(v) if v is not None else "" for v in columns ]
                        if not nodate:
                            print str(dt) + ",",
                        print ",".join(columns)
                columns = [None] * len(seriesids)
            else:
                columns[i] = value
            thisdt = dt
        if todate and not nodate:
            if excel:
                td = td.replace("T"," ")
            else:
                td = td + "Z"
            print "%s%s" % (td, "," * len(seriesids))
    elif state == "graphjson":
        # note: this snippet returns JSON, not HTML
        # https://google-developers.appspot.com/chart/interactive/docs/reference#dataparam
        if 'tqx' in form:
            tqx = dict([ kv.split(":") for kv in form["tqx"].value.split(";") ])
        else:
            tqx = {'reqId':0}
        fromdate = form["from"].value
        todate = form["to"].value
        vq = valuesquery
        fd = date_chars_only(iso8601(fromdate))
        if fd.find(" ") < 0:
            fd += "T00:00:00"
        else:
            fd = fd.replace(" ","T")
        fddt = datetime.datetime(*time.strptime(fd, "%Y-%m-%dT%H:%M:%S")[:6])
        td = date_chars_only(iso8601(todate))
        if td.find(" ") < 0:
            td += "T23:59:59"
        else:
            td = td.replace(" ","T")
        tddt = datetime.datetime(*time.strptime(td, "%Y-%m-%dT%H:%M:%S")[:6])
        if fromdate: vq += " and DateTimeUTC >= '%s'" % fd
        if   todate: vq += " and DateTimeUTC <= '%s'" % td
        vq += ';'
        print "Content-Type: application/json\n"
        if type(form["seriesid"]) == types.ListType:
            seriesids = form["seriesid"]
            titles = form["title"]
        else:
            seriesids = [form["seriesid"]]
            titles = [form["title"]]
        print "google.visualization.Query.setResponse({'reqId':'%s', 'status':'OK', 'table': {cols:[" % (tqx['reqId'])
        print "{label:'UTC',type:'datetime'},"
        for title in titles:
            t = title.value.translate(None, '\'"%&\\<>{}[]').replace(",","")
            print "{label:'%s',type:'number'}," % (t)
        print "],rows:[ "
        # do we need to output a full set of columns on each row?
        if fromdate:
            print "{c:[{v:%s}]}," % (datetimeToGSON(fddt))
        pairs = []
        for i, series in enumerate(seriesids):
            cur.execute(vq, series.value)
            for row in cur.fetchall():
                (dt, value) = row
                dt += datetime.timedelta(seconds = 59) # round up to next minute (only needed for last measurement of the hour)
                dt -= datetime.timedelta(minutes = dt.minute % 5, seconds = dt.second) # round down to current multiple of 5 minutes.
                pairs.append( (dt, value, i) )
        pairs.sort()
        thisdt = None
        for dt, value, i in pairs:
            if dt != thisdt:
                if thisdt is not None:
                    columns[i] = value
                    columns = [ str(v) if v is not None else "" for v in columns ]

                    print "{c:[{v:%s}," % datetimeToGSON(dt) + ",".join([ "{v:%s}" % c for c in columns]) + "]},"
                columns = [None] * len(seriesids)
            else:
                columns[i] = value
            thisdt = dt
        if todate:
            print "{c:[{v:%s}]}," % (datetimeToGSON(tddt))

        print "]}});" # end the rows, end the table, end the object, close the function

    else:
        print "Content-Type: text/html\n"
        print "Well, THAT broke quickly."

main()

