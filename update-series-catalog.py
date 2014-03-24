#!/usr/bin/python
# vim: set ai sw=4 sta fo=croql ts=8 expandtab syntax=python
                                                                                # die, PEP8's 80-column punched card requirement!

import time
import sys
import os
import re
import MySQLdb
import datetime
import json

def tname(tn):
    return tn.lower()

# This function updates all entries in the 
# SeriesCatalog by extracting the aggregate values
# from the dataValues table and from related tables.
def db_UpdateSeriesCatalog_All(con, cur):
  
    result_status = { "inserted": 0, "updated": 0 }

    
    #row =  (1L, 231L, 1L, 1L, 0L)
    #row =  (34L, 232L, 1L, 1L, 0L)
    #status = db_UpdateSeriesCatalog(con, cur, *row)
  
    query = ('SELECT MAX(SiteID), MAX(VariableID), MAX(MethodID), MAX(SourceID), MAX(QualityControlLevelID) FROM '
        + tname('DataValues')
        + ' GROUP BY SiteID, VariableID, SourceID, MethodID, QualityControlLevelID')
  
    print query
    result = cur.execute(query)
    print result

    for row in cur.fetchall():
        print "executing db_UpdateSeriesCatalog",row
        status = db_UpdateSeriesCatalog(con, cur, *row)
        result_status["inserted"] += status["inserted"]
        result_status["updated"] += status["updated"]
  
    print "rows inserted: %d" % result_status["inserted"]
    print "rows updated: %d" % result_status["updated"]


def db_find_seriesid(cur, siteID, variableID, methodID, sourceID, qcID):
    tn = tname('SeriesCatalog')
    query_text = """SELECT SeriesID FROM %(tn)s WHERE SiteID = %(siteID)s AND VariableID = %(variableID)s
                    AND MethodID = %(methodID)s AND SourceID = %(sourceID)s AND QualityControlLevelID = %(qcID)s;""" % locals()
  
    print "db_find_seriesid"
    print query_text
  
    num_rows = cur.execute(query_text)
   
    if num_rows == 0:
        return None
    else:
        return cur.fetchone()[0]

def db_UpdateSeriesCatalog(con, cur, siteID, variableID, methodID, sourceID, qcID):
  
    status = { "inserted": 0, "updated": 0 }
  
    #check for an existing seriesID
    series_id = db_find_seriesid(cur, siteID, variableID, methodID, sourceID, qcID)
  
    print "series_id:", series_id
  
    #run the values query - series catalog from data values table

    query = """
SELECT dv.SiteID, s.SiteCode, s.SiteName, s.SiteType,
dv.VariableID, v.VariableCode, v.VariableName, v.Speciation, 
v.VariableUnitsID, vu.UnitsName, 
v.SampleMedium, v.ValueType, v.TimeSupport, 
v.TimeUnitsID, tu.UnitsName, 
v.DataType, v.GeneralCategory,
m.MethodID, m.MethodDescription, 
sou.SourceID, sou.Organization, sou.SourceDescription, sou.Citation,
qc.QualityControlLevelID, qc.QualityControlLevelCode,

MIN( dv.LocalDateTime ) AS "BeginDateTime", MAX( dv.LocalDateTime ) AS "EndDateTime",
MIN( dv.DateTimeUTC )  AS "BeginDateTimeUTC", MAX( dv.DateTimeUTC )  AS "EndDateTimeUTC",
COUNT( dv.ValueID ) AS "ValueCount" 
FROM tn(DataValues) dv 
INNER JOIN tn(Sites) s ON dv.SiteID = s.SiteID 
INNER JOIN tn(Variables) v ON dv.VariableID = v.VariableID 
INNER JOIN tn(Units) vu ON v.VariableunitsID = vu.UnitsID 
INNER JOIN tn(Units) tu ON v.TimeunitsID = tu.UnitsID 
INNER JOIN tn(Methods) m ON dv.MethodID = m.MethodID 
INNER JOIN tn(Sources) sou ON dv.SourceID = sou.SourceID 
INNER JOIN tn(QualityControlLevels) qc ON dv.QualityControlLevelID = qc.QualityControlLevelID 
WHERE dv.SiteID = %(siteID)s
 AND dv.VariableID = %(variableID)s
 AND dv.MethodID = %(methodID)s
 AND dv.SourceID = %(sourceID)s
 AND dv.QualityControlLevelID = %(qcID)s""" % locals()

    # turn tn(X) into the appropriate tablename
    query = re.sub(r'tn\((.*?)\)', lambda tn:tname(tn.group(1)), query)

    num_rows = cur.execute(query)
   
    #print query
  
    if num_rows == 0:
        print "NO SERIES IDENTIFIED from DataValues TABLE!"
        return
  
    # find entries to SeriesCatalog from joining DataValues and other tables
    # engage in hackery to keep the variableUnitsName and timeUnitsName distinct.
    results = {}
    row = cur.fetchone()
    for i, column in enumerate(cur.description):
        if i == 9:
            results['variableUnitsName'] = row[i]
        elif i == 14:
            results['timeUnitsName'] = row[i]
        else:
            results[column[0]] = row[i]

    fields = results.keys()
    fields.sort()

    # some columns are arbitrary text
    for f in 'SiteName MethodDescription SourceDescription Organization Citation'.split():
        results[f] = con.escape_string(results[f])
    for f in 'BeginDateTime BeginDateTimeUTC EndDateTime EndDateTimeUTC'.split():
        results[f] = results[f].isoformat()

    if series_id is None:
        # insert a new seriescatalog record.
        insert = 'INSERT INTO %s (%s) VALUES (%s);' % (
            tname('SeriesCatalog'),
            ", ".join(fields),
            ", ".join(["'%s'" % results[k] for k in fields])
            )

	print insert
        num_rows = cur.execute(insert)
        print "inserted",num_rows 
        status["inserted"] = 1
    else:
        update = 'UPDATE %s SET %s WHERE SeriesID = %s;' % (
            tname('SeriesCatalog'),
            ", ".join(["%s = '%s'" % (k, results[k]) for k in fields] ),
            series_id
            )
        print update
        num_rows = cur.execute(update)
        print "updated",num_rows 
        status["updated"] = 1
    return status

if __name__ == "__main__":
    import getopt

    if os.path.exists("config.json"):
        config = json.load(open("config.json"))
    else:
        config = {}

    opts, args = getopt.getopt(sys.argv[1:], "wh:d:u:p:", ["write", "host=", "passwd=", "db=", "user="])
    write = False
    for n,v in opts:
        if n == "-h" or n == "--host":
            config["host"] = v
        if n == "-p" or n == "--passwd":
            config["passwd"] = v
        if n == "-d" or n == "--db":
            config["db"] = v
        if n == "-u" or n == "--user":
            config["user"] = v
        if n == "-w" or n == "--write":
            write = True

    if "passwd" not in config:
        config["passwd"] = getpass.getpass("Password: ")
    if "host" not in config:
        config["host"] = raw_input("Host: ")
    if "db" not in config:
        config["db"] = raw_input("Database: ")
    if "user" not in config:
        config["user"] = raw_input("User: ")

    if write:
        json.dump(config, open("config.json.new", 'w'))
        os.rename("config.json", "config.json.old")
        os.rename("config.json.new", "config.json")
        
    con = MySQLdb.connect(**config)
    cur = con.cursor()
    db_UpdateSeriesCatalog_All(con, cur)
    con.commit()


