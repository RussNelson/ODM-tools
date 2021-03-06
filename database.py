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
from sensors import rths_sites, rths_sensors, units
import datetime
import math
import pickle
try:
    import MySQLdb
except:
    pass
import json
import paramiko
import seawater # http://www.imr.no/~bjorn/python/seawater/index.html

class Datawriter: # base class
    """ provide methods for Dataparser to call to write data into a set of streams. """
    def __init__(self):
        pass

    def open(self, fields, model, serial, ymdh):
        """ Open a set of files, one for each of the fields (unless the filename is None)"""
        pass

    def write(self, dt, fieldsums):
        """ write this sample at its date to all fields (unless the output file is None) """
        pass

    def close(self):
        """ close this set of files. """
        pass

    def finish(self):
        """ finished adding anything. """
        pass

class DatawriterView(Datawriter):
    """ write just the last measurement out. """

    def open(self, fields, model, serial, ymdh):
        """ Open a set of files, one for each of the fields (unless the filename is None)"""
        self.fieldvalues = {}
        for fieldnum,field in enumerate(fields):
            self.fieldvalues[fieldnum] = [None, None, model, serial, field[1], field[2]]
        self.forcemethodname = None
 
    def forcemethod(self, name):
        self.forcemethodname = name

    def write(self, dt, fieldsums):
        """ write this sample at its date to all fields (unless the output file is None) """
        timestamp = dt.strftime("%m/%d/%Y %H:%M:%S")
        for fieldnum,field in self.fieldvalues.items():
            if fieldsums[fieldnum][0]:
                field[0] = timestamp
                field[1] = fieldsums[fieldnum][1] / fieldsums[fieldnum][0]

    def close(self):
        for fieldnum,field in self.fieldvalues.items():
            if field[0] is not None and field[4]:
                sys.stdout.write("%s,%s,%s,%.3f %s\n" % (field[2], field[3], field[4], field[1], units[int(field[5])]))

class DatawriterCSV(Datawriter):
    """ provide methods for Dataparser to call to write data into a set of streams. """

    def _makefn(self, fn, serial, ymdh):
        """ Construct the destination filename
        >>> writercsv._makefn("a", "b", "YYYYMMDD")
        'a-b.csv'
        """
        return fn + "-%s.csv" % serial

    def open(self, fields, model, serial, ymdh):
        """ Open a set of files, one for each of the fields (unless the filename is None)"""
        self.fieldfiles = {}
        for fieldnum,field in enumerate(fields):
            title, fn = field[0:2] #rths_sensors
            outf = fn and open(self._makefn(fn, serial, ymdh), "w")
            if outf: outf.write("Date,%s\n" % title)
            self.fieldfiles[fieldnum] = outf

    def write(self, dt, fieldsums):
        """ write this sample at its date to all fields (unless the output file is None) """
        tzoffset = -4 # DST can go to hell.
        dt += datetime.timedelta(hours=tzoffset)
        timestamp = dt.strftime("%m/%d/%Y %H:%M:%S")
        for fieldnum,outf in self.fieldfiles.items():
            if outf and fieldsums[fieldnum][0]:
                outf.write("%s,%.3f\n" % (timestamp, fieldsums[fieldnum][1] / fieldsums[fieldnum][0]))

class DatawriterCSVone(DatawriterCSV):
    """ like CSV, but one for one hour's worth of data, and passes the date through to the name """
    def _makefn(self, fn, serial, ymdh):
        """ Construct the destination filename
        >>> writercsvone._makefn("a", "b", "YYYYMMDD")
        'rths/YYYY/YYYYMM/a-b-YYYYMMDD.csv'
        >>> 
        """
        return "rths/%s/%s/" % (ymdh[0:4], ymdh[0:6]) + fn + "-%s-%s.csv" % (serial, ymdh)

class DatawriterSQL(Datawriter):
    """ provide methods for Dataparser to call to write data into a set of streams. """

    def __init__(self, sitecode, config):
        Datawriter.__init__(self)
        # using: ssh -n -N  -L 3307:localhost:3306 -i /root/.ssh/id_dsa mysqlfwd@www.ra-tes.org
        self.config = config
        self.con = MySQLdb.connect(**self.config)
        self.cur = self.con.cursor()
        self.sitecode = sitecode
        self.fieldcolumns = None
        self.SiteID = {}
        self.VariableID = {}
        self.MethodID = {}
        self.forcemethodname = None
        self.delete = False
 
    def deleting(self):
        self.delete = True

    def forcemethod(self, name):
        self.forcemethodname = name

    def open(self, fields, model, serial, ymdh):
        self.fieldcolumns = []
        for fieldnum,field in enumerate(fields):
            variablecode = field[1] # rths_sensors
            methoddescription = field[4] # rths_sensors
            if methoddescription == '':
                if self.forcemethodname is None:
                    methoddescription = 'Autonomous Sensing'
                else:
                    methoddescription = self.forcemethodname
            else:
                if self.forcemethodname is not None:
                    methoddescription += " " + self.forcemethodname
            self.fieldcolumns.append([self.sitecode, variablecode, methoddescription, 0, None])
        self.fc = {}

    def write(self, dt, fieldsums):
        for i,fieldcolumn in enumerate(self.fieldcolumns):
            sitecode, variablecode, methoddescription = fieldcolumn[0:3]
            if variablecode and fieldsums[i][0]:
                self._insert(sitecode, variablecode, methoddescription, dt, fieldsums[i][1] / fieldsums[i][0])
                fieldcolumn[3] += 1
                fieldcolumn[4] = dt
            if variablecode == 'precip' and fieldsums[i][0]:
                self._add_precip(sitecode, variablecode, methoddescription, dt, fieldsums[i][1] / fieldsums[i][0])
                
    def _insert(self, SiteCode, VariableCode, MethodDescription, dt, value):

        id = (SiteCode, VariableCode, MethodDescription)
        if id in self.fc:
            self.fc[id].append( (dt, value) )
        else:
            self.fc[id] = [ (dt, value) ]

    def close(self):

        for id,dtval in self.fc.items():
            
            tzoffset = -5 # DST can go to hell.
            SiteID, VariableID, MethodID = self._get_site_variable(*id)
            if self.delete:
                ddd = ""
                for v in dtval:
                    ddd += ",'%s'" % v[0].isoformat()
                sqlcmd = """DELETE FROM datavalues WHERE 
                SiteID = %ld AND
                DateTimeUTC in (%s) AND
                VariableID = %ld AND
                MethodID = %ld AND
                SourceID = 1;""" % (SiteID, ddd[1:], VariableID, MethodID)
                self.cur.execute(sqlcmd)
            values = [ (
                SiteID,
                (x[0] + datetime.timedelta(hours=tzoffset)).isoformat(),
                tzoffset,
                x[0].isoformat(),
                VariableID,
                x[1],
                MethodID
            ) for x in dtval ]

            vvv = ""
            for v in values:
                vvv += ",(%ld,'%s',%d,'%s',%ld,%f,%ld,1,'nc')" % v
            #values = [ "(%ld,'%s',%d,'%s',%ld,%f,%ld,1,'nc')" % v in values ]

            sqlcmd = """
                INSERT IGNORE INTO datavalues
                (SiteID,LocalDateTime,UTCOffset,DateTimeUTC,VariableID,
                 DataValue,MethodID,SourceID,CensorCode)
                VALUES %s
            ; """ % vvv[1:]
            self.cur.execute(sqlcmd)

        if self.fieldcolumns is None: return
        for i,fieldcolumn in enumerate(self.fieldcolumns):
            site, variable, method, count, dt = fieldcolumn
            if count:
                print site,variable,method,dt,count
                self._update(site, variable, method, dt, count)
        self.con.commit()

    def _update(self, SiteCode, VariableCode, MethodDescription, dt, count):
        UTCtime = dt.isoformat()
        tzoffset = -5 # DST can go to hell.
        dt += datetime.timedelta(hours=tzoffset)
        ESTtime = dt.isoformat()
        sqlcmd = """
            UPDATE seriescatalog
            SET EndDateTime = '%(ESTtime)s',
                EndDateTimeUTC = '%(UTCtime)s',
                ValueCount = ValueCount + %(count)s
            WHERE SiteCode = '%(SiteCode)s'
            AND VariableCode = '%(VariableCode)s'
            AND MethodDescription = '%(MethodDescription)s'
        ; """ % locals()

        return self.cur.execute(sqlcmd)

    def _get_site_variable(self, SiteCode, VariableCode, MethodDescription):
        """ given a SiteCode and VariableCode, return a SiteID, VariableID, and MethodID """

        if SiteCode not in self.SiteID or VariableCode not in self.VariableID or MethodDescription not in self.MethodID:
            sqlcmd = """
                SELECT sites.SiteID, variables.VariableID, methods.MethodID
                FROM sites, variables, methods
                WHERE sites.SiteCode = '%(SiteCode)s'
                AND variables.VariableCode = '%(VariableCode)s'
                AND methods.MethodDescription = '%(MethodDescription)s'
                ; """ % locals()
            self.cur.execute(sqlcmd)

            svm= self.cur.fetchone()
            try:
                (self.SiteID[SiteCode], self.VariableID[VariableCode], self.MethodID[MethodDescription]) = svm
            except TypeError:
                print "one of these wasn't found:", SiteCode, VariableCode, MethodDescription
                raise
            except ValueError:
                print "one of these wasn't found:", SiteCode, VariableCode, MethodDescription
                raise
        return (self.SiteID[SiteCode], self.VariableID[VariableCode], self.MethodID[MethodDescription])

    
    def _add_precip(self, SiteCode, VariableCode, MethodDescription, dt, value):
        """ When we add a precip sample, update the affected daily sums"""
        """ For hour T, we need to sum from 0 to T inclusive, updating T through max(T). """

        SiteID, VariableID1, MethodID1 = self._get_site_variable(SiteCode, VariableCode, MethodDescription)
        VariableID2, MethodID2 = self._get_site_variable(SiteCode, 'precipdaily', "Daily Accumulation")[1:3]

        UTCtime = dt.isoformat()
        tzoffset = -5 # DST can go to hell.
        est = dt + datetime.timedelta(hours=tzoffset)
        ESTtime = est.isoformat()

        day = dt.date()

        # get the existing values into a dict
        sqlcmd = """
            SELECT DateTimeUTC, DataValue
            FROM datavalues
            WHERE SiteID = %(SiteID)s
            AND VariableID = %(VariableID1)s
            AND MethodID = %(MethodID1)s
            AND DateTimeUTC between '%(day)s 00:00:00' and '%(day)s 23:59:59'
            ; """ % locals()
        self.cur.execute(sqlcmd)

        datavalues = {}
        for hour, value in self.cur.fetchall():
            datavalues[hour.hour] = value

        if datavalues:
          for hour in range(dt.hour, max(datavalues.keys()) + 1):
            # sum from zero to hour and insert/update.
            sum = 0.0
            for h in range(0, hour + 1):
                if h in datavalues: sum += datavalues[h]

            dailyutc = datetime.datetime(dt.year, dt.month, dt.day, hour, 0, 0)
            dailyest = dailyutc + datetime.timedelta(hours=tzoffset)

            # see if we have an existing value that we're updating.
            sqlcmd = """
                SELECT ValueID
                FROM datavalues
                WHERE SiteID = %(SiteID)s
                AND VariableID = %(VariableID2)s
                AND MethodID = %(MethodID2)s
                AND DateTimeUTC = '%(dailyutc)s';
                ; """ % locals()
            self.cur.execute(sqlcmd)
            valueid = self.cur.fetchone()

            if valueid is None:
                # insert a new record.
                insert = """INSERT INTO datavalues
                    (SiteID,LocalDateTime,UTCOffset,DateTimeUTC,VariableID,
                        DataValue,MethodID,SourceID,CensorCode)
                    VALUES (%(SiteID)ld,'%(dailyest)s',%(tzoffset)d,'%(dailyutc)s',%(VariableID2)ld,
                        %(sum)f,%(MethodID2)ld,1,'nc')
                    ; """ % locals()
                self.cur.execute(insert)
            else:
                # update an existing record.
                valueid = valueid[0]
                update = 'UPDATE datavalues SET DataValue = %(sum)f WHERE ValueID = %(valueid)s;' % locals()
                self.cur.execute(update)
        
class Dataparser:

    def __init__(self, rthssi):
        self.rthssi = rthssi
        self.count = 0
        self.minuteperiod = None

    def parse_first_fn(self, fn):
        """ remember things taken from the first filename
        >>> data.parse_first_fn("/home/r5/log-MBIRDS-3-2011120423.gz")
        >>> data.model
        'MBIRDS'
        >>> data.serial
        '3'
        >>> data.YMDH
        '2011120423'
        >>> 
        >>> data.parse_first_fn("/home/r5/log-MBIRDS-3-2011120423")
        >>> data.model
        'MBIRDS'
        >>> data.serial
        '3'
        >>> data.YMDH
        '2011120423'
        >>> 
        """
        fnmain = os.path.splitext(os.path.basename(fn))
        fnfields = fnmain[0].split('-')
        try:
            self.model = fnfields[1]
            self.serial = fnfields[2]
            self.YMDH = fnfields[3] # YYYYMMDDHH
        except IndexError: 
            print fn
            raise

    def parse_fn(self, fn):
        """ pull the year, month, and day out of the filename
        >>> data.parse_fn("/home/r5/log-MBIRDS-3-2011120423.gz")
        >>> data.YMD
        '20111204'
        >>> data.parse_fn("/data/log-MBIRDS-3-2011120423")
        >>> data.YMD
        '20111204'
        >>> 
        """
        fnmain = os.path.splitext(os.path.basename(fn))
        fnfields = fnmain[0].split('-')
        self.YMD = fnfields[3][:8] # YYYYMMDD
        self.YMDH = fnfields[3][:10] # YYYYMMDDHH
        self.dst = None
 
    dst2012end = datetime.datetime(2012, 11, 4, 1) # actually at 2AM, but it all goes into the 1AM file.
    dst2012endnext = datetime.datetime(2012, 11, 4, 2)
    dst2013begin = datetime.datetime(2013, 3, 10, 2)
    dst2013end = datetime.datetime(2013, 11, 3, 2)
    dst2014begin = datetime.datetime(2014, 3, 9, 2)

    def set_dt_fields(self, fields):
        """ remember the full UTC datetime for each line, and return the data fields.
        the data files were recorded in Eastern time, complete with DST crap.
        each line is of the form: 10:06:19 4.59,999.00,257
        we get it pre-split into two fields at the space.

        >>> # ---------------------------------------------------------------
        >>> fn = "/home/s11/log-MBIRDS-3-2011120423.gz"
        >>> data.utc = False
        >>> data.dt = None
        >>> data.parse_fn(fn)
        >>> data.set_dt_fields("23:00:00 5.29,999.00,262".split())
        ['5.29', '999.00', '262']
        >>> data.dt
        datetime.datetime(2011, 12, 5, 3, 0)
        >>> data.set_dt_fields("23:00:37 5.29,999.00,262".split())
        ['5.29', '999.00', '262']
        >>> data.dt
        datetime.datetime(2011, 12, 5, 3, 0, 37)
        >>> # ---------------------------------------------------------------
        >>> # check the transition from daylight savings to standard (fall back)
        >>> fn = "/home/s21/log-windair-12-2012110400.gz"
        >>> data.utc = False
        >>> data.parse_fn(fn)
        >>> data.set_dt_fields("00:00:00 0.00,0.00,19.69,33.13".split())
        ['0.00', '0.00', '19.69', '33.13']
        >>> data.dt
        datetime.datetime(2012, 11, 4, 4, 0)
        >>> data.set_dt_fields("00:59:58 0.00,0.00,19.71,33.02".split())
        ['0.00', '0.00', '19.71', '33.02']
        >>> data.dt
        datetime.datetime(2012, 11, 4, 4, 59, 58)
        >>> fn = "/home/s21/log-windair-12-2012110401.gz"
        >>> data.dt = None
        >>> data.parse_fn(fn)
        >>> data.set_dt_fields("00:59:58 0.00,0.00,19.71,33.02".split())
        ['0.00', '0.00', '19.71', '33.02']
        >>> data.dt
        datetime.datetime(2012, 11, 4, 4, 59, 58)
        >>> data.set_dt_fields("01:00:00 0.00,0.00,19.72,33.01".split())
        ['0.00', '0.00', '19.72', '33.01']
        >>> data.dt
        datetime.datetime(2012, 11, 4, 5, 0)
        >>> data.set_dt_fields("01:59:58 0.00,0.00,19.66,33.01".split())
        ['0.00', '0.00', '19.66', '33.01']
        >>> data.dt
        datetime.datetime(2012, 11, 4, 5, 59, 58)
        >>> data.set_dt_fields("01:00:00 0.00,0.00,19.67,32.98".split())
        ['0.00', '0.00', '19.67', '32.98']
        >>> data.dt
        datetime.datetime(2012, 11, 4, 6, 0)
        >>> data.set_dt_fields("01:59:58 0.00,0.00,19.71,33.02".split())
        ['0.00', '0.00', '19.71', '33.02']
        >>> data.dt
        datetime.datetime(2012, 11, 4, 6, 59, 58)
        >>> 
        >>> # ---------------------------------------------------------------
        >>> # check the transition from standard to daylight savings (spring ahead)
        >>> fn = "/home/s3/done-2013-10-14/log-windair-11-2013031001.gz"
        >>> data.utc = False
        >>> data.parse_fn(fn)
        >>> data.set_dt_fields("01:00:00 0.00,0.00,-4.87,91.12".split())
        ['0.00', '0.00', '-4.87', '91.12']
        >>> data.dt
        datetime.datetime(2013, 3, 10, 6, 0)
        >>> data.set_dt_fields("01:59:58 0.00,0.00,-5.67,91.76".split())
        ['0.00', '0.00', '-5.67', '91.76']
        >>> data.dt
        datetime.datetime(2013, 3, 10, 6, 59, 58)
        >>> data.set_dt_fields("03:00:00 0.00,0.00,-5.66,91.76".split())
        ['0.00', '0.00', '-5.66', '91.76']
        >>> data.dt
        datetime.datetime(2013, 3, 10, 7, 0)
        >>> 

        """
        now = datetime.datetime(*time.strptime(self.YMD+fields[0], "%Y%m%d%H:%M:%S")[:6])
        if self.utc:
            tzoffset = 0 # wasn't that easy?
        elif now < self.dst2012end: # DST 2012
            tzoffset = -4
        elif now < self.dst2012endnext: # the last hour of DST 2012
            if self.dst is None or now > self.dst:
                tzoffset = -4
                self.dst = now # watch for wrap-around
            else:
                tzoffset = -5
        elif now < self.dst2013begin: # ST 2012-2013
            tzoffset = -5
        elif now < self.dst2013end: # DST 2013
            tzoffset = -4
        elif now < self.dst2014begin: # ST 2013-2014
            tzoffset = -5
        else: # DST 2014-2014
            tzoffset = -4
        now += datetime.timedelta(hours=-tzoffset)
        if self.dt is not None and now < self.dt: raise ValueError, "time cannot go backwards"
        self.dt = now
        return fields[1].split(',')

    def date_parse(self, dstr):
        """ take a MM/DD/YYYY date and turn it into a datetime.date
        >>> data.date_parse("10/20/2013")
        datetime.datetime(2013, 10, 20, 0, 0)
        >>> 
        """
        fields = dstr.split("/")
        return datetime.datetime(int(fields[2]), int(fields[0]), int(fields[1]), 0, 0, 0)

    def get_calibration(self, model, serial, date):
        """ find the right calibration value. Crap out if not found.
        >>> data.get_calibration("pH", "0", datetime.datetime(2013, 7, 31, 0,0,0))
        ['ph', '0', 'for St. Regis', '', '5/20/2013', '-0.8766', '31.99', '', '', '', '', '', '', '', '', '', '']
        >>> data.get_calibration("pH", "0", datetime.datetime(2013, 8, 31, 0,0,0))
        ['ph', '0', 'Grasse', '', '8/15/2013', '-1.0634', '37.28', '', '', '', '', '', '', '', '', '', '']
        >>> data.get_calibration("pdepth2", "47", datetime.datetime(2014, 12, 8, 0,0,0))
        ['ph', '0', 'Grasse', '', '8/15/2013', '-1.0634', '37.28', '', '', '', '', '', '', '', '', '', '']
        >>> 
        """
        model = model.lower()
        for line in self.rthssi:
            if line[0].lower() == model and line[1] == serial:
                if line[4] == "":
                    return line
                dt = self.date_parse(line[4])
                if dt < date: # only if it was calibrated before this measurement.
                    latest = line
        if 'latest' not in locals():
            print "unable to get calibration for",model,serial,date
        return latest # if there was none, we raise an exception.

    def normalize_fieldsums(self, fieldsums):
        """ normalize the sums as needed for calibration constants, etc """
        pass

    def prepare_for(self, fn):
        """ do whatever is necessary to prepare for accessing this file."""
        pass

    def add_to_fieldsums(self, fieldsums, fields):
        """add the fields to the sums."""
        for i in range(len(fields)):
            try:
                fieldsums[i][1] += float(fields[i])
            except ValueError:
                pass
            else:
                fieldsums[i][0] += 1

    def Xdoneaveraging(self):
        return True

    def Xdoneaveraging(self):
        self.count += 1
        self.count %= 21
        return self.count == 0

    def doneaveraging(self):
        """ average five minutes worth of samples. """
        done = self.minuteperiod is not None and self.dt.minute / 5 != self.minuteperiod
        self.minuteperiod = self.dt.minute / 5
        return done

    def zero_fieldsums(self):
        return [
            [0 for col in range(2)]
            for row in rths_sensors[self.model]
            ]

    def dofiles(self, files, writer):
        """ given a list of files, create running averages and split out by sensor."""

        fn = files[0]
        self.parse_first_fn(fn)
        if self.model not in rths_sensors:
            return # not a model that has been described to us.
        writer.open(rths_sensors[self.model], self.model, self.serial, self.YMDH)
        fieldsums = self.zero_fieldsums()

        self.dt = None # keep track of the latest timestamp found.
        for fn in files:
            try:
                fn = fn.rstrip()
                self.utc = fn.find("/r") >= 0
                self.parse_fn(fn)
                #writer.hour(self.YMDH, self.period)

                self.prepare_for(fn)
             
                try:
                    lines = gzip.open(fn).readlines()
                except IOError, err:
                    if err[0] != 'Not a gzipped file':
                        print err
                        raise
                    lines = open(fn).readlines()
                for line in lines:
                    fields = line.split()
                    if len(fields) < 2: continue
                    if fields[0] == "starting": continue
                    try:
                        fields = self.set_dt_fields(fields)
                    except ValueError:
                        continue # if the date cannot be parsed
                    if len(fields) != len(rths_sensors[self.model]): continue
                    self.add_to_fieldsums(fieldsums, fields)
                    if self.doneaveraging():
                        self.normalize_fieldsums(fieldsums)
                        writer.write(self.dt, fieldsums)
                        fieldsums = self.zero_fieldsums()
            except:
                sys.stderr.write(fn + "\n")
                raise
        # if we have any data, finish off any remaining samples
        if sum([f[0] for f in fieldsums]):
            try:
                self.normalize_fieldsums(fieldsums)
                writer.write(self.dt, fieldsums)
            except:
                sys.stderr.write("probably caused by this file:\n%s\n" % files[-1] )
                raise
        writer.close()

        if self.dt is not None:
            # remember the most recently found timestamp in a file.
            lastrx = time.mktime(self.dt.timetuple()) 
            fn = "timestamp-%s-%s" % (self.model, self.serial)
            open(fn, "w")
            os.utime(fn, (lastrx, lastrx))

class Datadepth(Dataparser):
    def normalize_fieldsums(self, fieldsums):
        """ use centimeters """
        fieldsums[0][1] *= 100.0

class Datadepth2(Dataparser):
    def normalize_fieldsums(self, fieldsums):
        """ use centimeters, and adjust calibration on the low-range sensor. """
        fieldsums[0][1] /= fieldsums[0][0]
        fieldsums[0][1]  = fieldsums[0][1] / 3 + 0.94
        fieldsums[0][0]  = 1
        fieldsums[0][1] *= 100.0
        fieldsums[1][1] *= 100.0

class Datavoltage(Dataparser):
    """ read the battery voltage data. For historical reasons it's not in
    exactly the same file format.  If the sensor's home directory contains
    .grid, ignore the voltage data. Some sensors, both technologics and
    raspberry Pi, will upload bogus voltage data even if they're on the grid.
    """

    def parse_first_fn(self, fn):
        """ we have to get the "serial" number from the filename """
        # /home/s8/voltage-2012-10-18.txt.gz
        # /home/s8/voltage-2013-05-04-13.txt.gz
        self.model = 'voltage'
        fnfields = fn.split('/')
        self.serial = fnfields[2] # actually the station name.
        fn = os.path.join(fnfields[0], fnfields[1], fnfields[2], ".grid")
        self.ignore = os.path.exists(fn)
        dashfields = fnfields[3].split('-')
        self.YMDH = "".join(dashfields[1:4])
        if len(dashfields) == 5:
            # either way, YMDH is going to be unique
            self.YMDH += dashfields[4].split('.')[0]

    def parse_fn(self, fn):
        # every line in a voltage file has the YMD, but set up YMDH.
        pass

    def normalize_fieldsums(self, fieldsums):
        if self.ignore:
            for f in fieldsums:
                f[0] = 0
            return
        if fieldsums[0][0]:
            if fieldsums[0][1] / fieldsums[0][0] < 3:
                # map the value from an A/D value into a voltage
                fieldsums[0][1] *= 7.25

    def set_dt_fields(self, fields):
        """ parse a ctime timestamp and return the voltage field """
        # Wed Mar 12 18:50:55 2014 12.112 0.000 0.217
        t = " ".join(fields[1:5])
        then = time.strptime(t, "%b %d %H:%M:%S %Y")
        self.dt = datetime.datetime.fromtimestamp(time.mktime(then))
        if self.dt is None:
            raise ValueError, "unparsable date %s,%s" % (then, time.mktime(then))
        # append some extra fields in case we have short columns.
        fields.append("")
        fields.append("")
        return fields[5:8]

class Datapdepth(Dataparser):
    """ read the data produced by a pdepth. We have to throw out bad temperature samples. """
    def __init__(self, rthssi):
        Dataparser.__init__(self, rthssi)
        self.lastavgt = None

    def add_to_fieldsums(self, fieldsums, fields):
        # add the depth.
        fieldsums[0][0] += 1
        fieldsums[0][1] += float(fields[0])
        # throw out temperature samples which are more than +/- 1 degree different than last average.
        temp = float(fields[1])
        if self.lastavgt is None or self.lastavgt - 1 < temp < self.lastavgt + 1:
            fieldsums[1][0] += 1
            fieldsums[1][1] += temp

    def normalize_fieldsums(self, fieldsums):
        # remember the last average, but ignore the first one.
        count = fieldsums[1][0]
        if count:
            if self.lastavgt is None:
                fieldsums[1][0] = 0
            # in case we get a run of bad samples, don't throw off the average.
            self.lastavgt = fieldsums[1][1] / count
        if fieldsums[0][0]:
            # average and convert counts to centimeters.
            fieldsums[0][1] /= fieldsums[0][0]
            fieldsums[0][0] = 1
            inches = 0.1328 * fieldsums[0][1] + 1.1033
            fieldsums[0][1] = inches * 2.54

def find_or_in_done(fullfn, fn, tfn):
    """ look in the same folder of one fn for another fn, or below it in .../done, or return None
    >>> fn = "/tmp/log-pdepth2-14-2014040100.gz"
    >>> find_or_in_done(fn, r'pdepth[12]-.*-', 'pbar*-*-')
    >>> pfn = "/tmp/log-pbar-4-2014040100.gz"
    >>> donedir = "/tmp/done"
    >>> pdfn = os.path.join(donedir, os.path.basename(pfn))
    >>> open(pfn, "w").write('')
    >>> find_or_in_done(fn, r'pdepth[12]-.*-', 'pbar*-*-')
    '/tmp/log-pbar-4-2014040100.gz'
    >>> os.mkdir(donedir)
    >>> os.unlink(pfn)
    >>> open(pdfn, "w").write('')
    >>> find_or_in_done(fn, r'pdepth[12]-.*-', 'pbar*-*-')
    '/tmp/done/log-pbar-4-2014040100.gz'
    >>> pdfn2 = os.path.join(donedir, "log-pbar-5-2014040100.gz")
    >>> open(pdfn2, "w").write('')
    >>> find_or_in_done(fn, r'pdepth[12]-.*-', 'pbar*-*-')
    '/tmp/done/log-pbar-4-2014040100.gz'
    >>> os.unlink(pdfn2)
    >>> os.unlink(pdfn)
    >>> os.rmdir(donedir)
    """
    pfn = os.path.basename(fullfn)
    pfn = re.sub(fn, tfn, pfn)
    for root, dirnames, filenames in os.walk(os.path.dirname(fullfn)):
        pbar_fns = fnmatch.filter(filenames, pfn)
        if len(pbar_fns) >= 1:
            return os.path.join(root, pbar_fns[0])
    return None

class Datapdepth1(Dataparser):
    # Have to put in a bit of explanation about our columns. We add on two
    # columns beyond what the sensor gives us. The first column is the water
    # level above sea level. The second column is the most recent pbar data.
    """ read the data produced by a pdepth1. We have to pull in pbar data to turn pressure into depth. """
    pcol = 5
    levelcol = 4

    def prepare_for(self, fn):
        """ Given a pdepth1 or pdepth2 filename, find the associated pbar file and read it into a dict"""
        pbar_fn = find_or_in_done(fn, r'pdepth[12]-.*-', 'pbar*-*-')
        self.pbars = {}
        self.last_pbar = None
        if pbar_fn is None:
            print "too many/few pbar_fns: %s" % fn
            return
        try:
            lines = gzip.open(pbar_fn).readlines()
        except IOError, err:
            if err[0] != 'Not a gzipped file': raise
            lines = open(pbar_fn).readlines()
        for line in lines:
            fields = line.split()
            if len(fields) != 2: continue
            if fields[1].startswith("reading"): continue
            self.pbars[fields[0]] = fields[1].rstrip()
        # get the first one, to make sure that we have one.
        ks = self.pbars.keys()
        ks.sort()
        if len(ks):
            self.last_pbar = self.pbars[ks[0]]

    def set_dt_fields(self, fields):
        """ get the datetime, but also remember HMS """
        self.hms = fields[0]
        retval = Dataparser.set_dt_fields(self, fields)
        retval.append(0) # create an additional column for water level.
        return retval

    def add_to_fieldsums(self, fieldsums, fields):
        # make room for the pbar sum.
        if len(fieldsums) < self.pcol + 1:
            fieldsums.append([0,0])
        """add the fields to the sums."""
        values = []
        for i in range(self.levelcol + 1):
            try:
                value = float(fields[i])
            except ValueError:
                values.append([0,0])
            else:
                values.append([1, value])
        # elide out-of-range values.
        if not 5 < values[1][1] < 1005:
            values[1][1] = 0
            values[1][0] = 0
        if not 5 < values[2][1] < 1005:
            values[2][1] = 0
            values[2][0] = 0
        for i in range(self.levelcol + 1):
            fieldsums[i][0] += values[i][0]
            fieldsums[i][1] += values[i][1]

        # pull pbar in. if there is not one to be had, don't output anything.
        if self.last_pbar is None: return # we shouldn't, but we might.
        if self.hms in self.pbars:
            self.last_pbar = self.pbars[self.hms]
        if self.last_pbar:
            pfields = self.last_pbar.split(',')
            if len(pfields) == 2:
                try:
                    b = float(pfields[1])
                except ValueError:
                    pass
                else:
                    fieldsums[self.pcol][0] += 1
                    fieldsums[self.pcol][1] += b

    def normalize_fieldsums(self, fieldsums):
        cline = self.get_calibration(self.model, self.serial, self.dt)
        # 5 psi temperature coefficient,5 psi load coefficient,15 psi temperature coefficient,15 psi load coefficient,offset, elevation
        calibration = [ float(re.sub(r' *^$', '0', v)) for v in cline[6:13] ]
        # normalize
        for f in fieldsums:
            if f[0]:
                f[1] /= f[0]
                f[0] = 1

        if calibration[0] == 0 and calibration[1] == 0 and calibration[4] == 0:
            # broken sensor - pretend it got no samples
            fieldsums[1][0] = 0

        if calibration[2] == 0 and calibration[3] == 0 and calibration[5] == 0:
            # broken sensor - pretend it got no samples
            fieldsums[2][0] = 0

        if self.pcol == 6 and fieldsums[3][0] and fieldsums[4][0] and fieldsums[3][1] > fieldsums[4][1]:
            # water temperature is always lower than board temperature.
            t = fieldsums[3][1]
            fieldsums[3][1] = fieldsums[4][1]
            fieldsums[4][1] = t

        if fieldsums[1][0]:
            baro = fieldsums[self.pcol][1]
            temp = fieldsums[3][1]
            fieldsums[1][1] = (fieldsums[1][1]+ ( 1013.25 - baro ) * 0.402 * calibration[1] + (temp - 35) * calibration[0]) / calibration[1] + calibration[4]
            fieldsums[1][1] *= 2.54
        if fieldsums[2][0]:
            baro = fieldsums[self.pcol][1]
            temp = fieldsums[3][1]
            fieldsums[2][1] = (fieldsums[2][1]+ ( 1013.25 - baro ) * 0.402 * calibration[3] + (temp - 35) * calibration[2]) / calibration[3] + calibration[5]
            fieldsums[2][1] *= 2.54
        fieldsums[0][0] = 0 # ignore first column

        if calibration[6]: # do we have elevation data?
            if fieldsums[1][0]: # use the most accurate data we have
                gage = fieldsums[1][1]
            else:
                gage = fieldsums[2][1]
            # convert Survey feet to meters: http://en.wikipedia.org/wiki/Foot_%28unit%29#Survey_foot
            fieldsums[self.levelcol][1] = calibration[6] * 1200.00 / 3937.0 + gage / 100.0
            fieldsums[self.levelcol][0] = 1
        else:
            fieldsums[self.levelcol][0] = 0 # we have no elevation, so we know not our water level.
            
class Datapdepth2(Datapdepth1):
    """ same data format, but we have btemperature in column 4. """
    levelcol = 5
    pcol = 6


class Datacond(Dataparser):
    """ we come in with three columns, but need to output conductivity in uS/cm and salinity in PSU. """

    def prepare_for(self, fn):
        """ Given a cond filename, find an associated pdepth file and read it into a dict"""
        pbar_fn = find_or_in_done(fn, r'COND-.*-', 'pdepth*-*-')
        self.pbars = {}
        self.last = None
        if pbar_fn is None:
            print "missing pdepth: %s" % fn
            return
        try:
            lines = gzip.open(pbar_fn).readlines()
        except IOError, err:
            if err[0] != 'Not a gzipped file': raise
            lines = open(pbar_fn).readlines()

        # fill self.pbars with the pdepth[12] water temperature.
        pcol = 4
        if pbar_fn.find("pdepth2") >= 0: pcol = 5
        for line in lines:
            fields = line.split()
            if len(fields) != 2: continue
            pfields = fields[1].split(',')
            if len(pfields) != pcol: continue
            try:
                t1 = float(pfields[3])
            except ValueError:
                continue
            if pcol == 5: # possible for inside and outside to be swapped.
                try:
                    t2 = float(pfields[4])
                except ValueError:
                    continue
                if t2 < t1: t1 = t2
            self.pbars[fields[0]] = t1
        print self.pbars
        # get the first one, to make sure that we have one.
        ks = self.pbars.keys()
        ks.sort()
        if len(ks):
            self.last = self.pbars[ks[0]]

    def set_dt_fields(self, fields):
        """ get the datetime, but also remember HMS """
        self.hms = fields[0]
        return Dataparser.set_dt_fields(self, fields)

    def add_to_fieldsums(self, fieldsums, fields):
        """add the fields to the sums."""
        # make room for the temperature sum.
        if len(fieldsums) < 4:
            fieldsums.append([0,0])
        # our version went through fieldcol+1; this goes through all fields.
        Dataparser.add_to_fieldsums(self, fieldsums, fields)

        # pull pbar in. if there is not one to be had, don't output anything.
        if self.last is None: return # we shouldn't, but we might.
        if self.hms in self.pbars:
            self.last = self.pbars[self.hms]
        if self.last:
            fieldsums[3][0] += 1
            fieldsums[3][1] += self.last

    def normalize_fieldsums(self, fieldsums):
        # missing values become 0.
        cline = self.get_calibration(self.model, self.serial, self.dt)
        calibration = [ float(re.sub(r' *^$', '0', v)) for v in cline[5:]]
        # get the average values
        for i in range(3):
            if fieldsums[i][0] == 0:
                # if any column has no samples, give up
                for i in range(3):
                    fieldsums[i][0] = 0
                return
        for i in range(3):
            if fieldsums[i][0]:
                fieldsums[i][1] /= fieldsums[i][0]
                fieldsums[i][0] = 0 # by default, discard them all.
        # look for the best value.
        for i in range(3):
            # look for a value in the middle range which has a calibration.
            if calibration[i*2]:
                # if we have a calibration for the two higher ranges, and the value fits,
                # use it, otherwise use the last one.
                if ((i == 0 and fieldsums[i][1] < 65535 * 0.90) or
                    (i == 1 and 65535/4 < fieldsums[i][1] < 65535 * 0.90) or
                    (i == 2)):
                    fieldsums[0][1] = calibration[i*2] * math.exp( calibration[i*2+1] * fieldsums[i][1])
                    fieldsums[0][0] = 1
                    break
            elif calibration[6 + i]:
                # if we have a better calibration for the linear mapping,
                # use it, otherwise use the last one.
                if ((i == 0 and fieldsums[i][1] < 65535 * 0.90) or
                    (i == 1 and 65535/4 < fieldsums[i][1] < 65535 * 0.90) or
                    (i == 2)):
                    fieldsums[0][1] = calibration[6 + i] * (65535.0 / fieldsums[i][1] - 1) + calibration[9 + i]
                    fieldsums[0][0] = 1
                    break
        else:
            #print "found nothing in",fieldsums, 65535/4 , 65535 * 0.75
            pass
        # if we have data, then compute the salinity.
        if fieldsums[0][0] and fieldsums[3][0]:
            fieldsums[1][1] = seawater.salt(fieldsums[0][1] / 42914.0, fieldsums[3][1] / fieldsums[3][0], 0)
            fieldsums[1][0] = 1

class Datafl3(Dataparser):
    def normalize_fieldsums(self, fieldsums):
        cline = self.get_calibration(self.model, self.serial, self.dt)
        calibration = map(float, cline[5:11])
        for i in range(2):
            col = [1,5][i]
            if fieldsums[col][0]:
                fieldsums[col][1] /= fieldsums[0][0]
                if False and fieldsums[col][1] < calibration[2 + 3 *i]: # if less than CWO, below detection limit.
                    fieldsums[col][0] = 0
                else:
                    fieldsums[col][1] = calibration[0 + 3 *i] * (fieldsums[col][1] - calibration[2 + 3 *i]) + calibration[1 + 3 *i]
                    fieldsums[col][0] = 1

class Dataph(Dataparser):
    def normalize_fieldsums(self, fieldsums):
        if fieldsums[0][0]:
            cline = self.get_calibration(self.model, self.serial, self.dt)
            calibration = map(float, cline[5:7])
            fieldsums[0][1] /= fieldsums[0][0]
            fieldsums[0][1] = calibration[0] * fieldsums[0][1] + calibration[1]
            fieldsums[0][0] = 1

class Datado3(Dataparser):
    def normalize_fieldsums(self, fieldsums):
        if fieldsums[0][0]:
            cline = self.get_calibration(self.model, self.serial, self.dt)
            calibration = map(float, cline[5:7])
            fieldsums[0][1] /= fieldsums[0][0]
            fieldsums[0][1] = calibration[0] * fieldsums[0][1] + calibration[1]
            fieldsums[0][0] = 1

class History:
    """a data structure for ppal history.
>>> fn = "/tmp/junkfile"
>>> if os.path.exists(fn):
...     os.unlink(fn)
>>> h = History("ppal-1", datetime.datetime.now(), fn=fn)
>>> h.previous
>>> h.dt
>>> h.raining
False
>>> h.previous = 27
>>> h.dt = datetime.datetime.now()
>>> h._history
{}
>>> h.dump()
>>> h = History("ppal-1", datetime.datetime.now(), fn=fn)
>>> h.previous
27
>>> h.raining
False
>>> len(h._history)
1
>>> h = History("ppal-1", datetime.datetime.now() + datetime.timedelta(hours=1, minutes=1), fn=fn)
>>> h.previous
>>>  """
    def __init__(self, modelserial, dt, fn="ppalhistory.pickle"):
        """ set up our data """
        self._modelserial = modelserial
        self._fn = fn
        if self.load() and self.dt < dt and self.dt + datetime.timedelta(hours = 1, minutes=1) >= dt:
            pass
        else:
            #if 'dt' in self.__dict__:
            #    print "skipping because",self.dt, dt
            self.raining = False
            self.previous = None
            self.dt = None # value never referenced if previous is None

    def load(self):
        """ load the history file and return true, or set the history empty and return false """
        if os.path.exists(self._fn):
            self._history = pickle.load(open(self._fn))
            if self._modelserial in self._history:
                (self.raining, self.previous, self.dt) = self._history[self._modelserial]
                return True
        else:
            self._history = {}
        return False

    def dump(self):
        """ if we have a history file, write it out with our new values."""
        if self._history is not None:
            self._history[self._modelserial] = (self.raining, self.previous, self.dt)
            pickle.dump(self._history, open(self._fn, "w") )

class Datappal(Dataparser):
    """ We analyze a set of samples looking for an increase from the previous level. If it's large enough,
    we conclude that it's raining. As long as that keeps going up, we keep counting it as rain. Once it
    decreases, we conclude that the rain has stopped, and we start looking for an increase in the level again.
    Making life interesting is that we need to keep a history of the state from run to run. If there is too
    long a gap from the previous history, we discard it and start over again."""

    threshold = 0.00975
    threshold = 0.0087 # rain period starts with a delta at least this big.
    threshold = 0.039  # per Jimmy 6/21/213

    def __init__(self, rthssi):
        Dataparser.__init__(self, rthssi)
        self.minuteperiod = None
        self.h = None

    def dofiles(self, files, writer):
        Dataparser.dofiles(self, files, writer)
        if self.h is not None:
            self.h.dump()

    def set_dt_fields(self, fields):
        """ return an extra copy of the high accuracy. """
        retval = Dataparser.set_dt_fields(self, fields)
        retval.append(retval[0])
        return retval

    def doneaveraging(self):
        """ average one hour worth of samples. """
        if False:
            done = self.minuteperiod is not None and self.dt.minute / 15 != self.minuteperiod
            self.minuteperiod = self.dt.minute / 15
        elif False:
            done = self.minuteperiod is not None and self.dt.hour != self.minuteperiod
            self.minuteperiod = self.dt.hour
        else:
            done = self.minuteperiod is not None and self.dt.day != self.minuteperiod
            self.minuteperiod = self.dt.day
        return done

    def normalize_fieldsums(self, fieldsums):
        cline = self.get_calibration(self.model, self.serial, self.dt)
        calibration = map(float, cline[5:7])
        if self.h is None:
            self.h = History(self.model + "-" + self.serial, self.dt)
        if fieldsums[0][0] == 0: return
        # only output deltas if the start of deltas exceeded threshold
        ave = float(fieldsums[0][1]) / fieldsums[0][0]
        this = ave  / calibration[0] # inches
        fieldsums[3][0] = 1;
        fieldsums[3][1] = this;
        if self.h.previous is None: # remember the first (but we should be carrying over from previous)
            self.h.previous = this
            self.h.dt = self.dt
            fieldsums[0][0] = 0
            return
        delta = this - self.h.previous
        self.h.previous = this
        self.h.dt = self.dt
        if delta > self.threshold:
            self.h.raining = True
        if self.h.raining and delta > 0:
            fieldsums[0][1] = delta * 25.4 # convert inches to mm
        else:
            self.h.raining = False
            fieldsums[0][1] = 0
        fieldsums[0][0] = 1

class Datappaltest(Dataparser):
    """ water temperature and 16-bit depth """

class Datappal1(Datappal):
    """ same data format """

class Datappal2(Datappal):
    """ same data format """

class Datappal3(Dataparser):
    """ temperature, temperature, detailed, extensive """

    def normalize_fieldsums(self, fieldsums):
        cline = self.get_calibration(self.model, self.serial, self.dt)
        calibration = map(float, cline[5:7])
        for i in range(2):
            ave = float(fieldsums[2+i][1]) / fieldsums[2+i][0]
            fieldsums[2+i][0] = 1
            fieldsums[2+i][1] = ave  / calibration[i]
 
class Dataoptode(Dataparser):
    """19:54:07 342.18,95.81,9.42,28.68,29.52,0.00,266.87,168.00,0.00,341.65"""

    def normalize_fieldsums(self, fieldsums):
        """After the timestamp (which is generated by the RTHS logger service), the next three fields are as follows:

        349.98 DO Concentration in micromolar
        97.57 DO Saturation in %
        9.24 Water Temperature in Celsius

        To convert from micromolar to mg/L, multiply the micromolar value by 0.032."""
        fieldsums[0][1] *= 0.032

class Dataobs(Dataparser):

    """07:00:26 0,485820,B,151.03,OBS,8,L,0,485821,R,81.96,271.22,000
    07:00:28 0,485822,G,93.80,3OBS,8,L,0,485823,B,151.21,405.59,000
    07:00:30 0,485824,R,77.60,OBS,8,L,0,485825,G,87.94,328.07,000
    07:00:32 0,485826,B,146.13,405.63,000
    07:00:32 0,485827,R,80.71,271.22,000
    07:00:33 0,485828,G,92.20,328.07,000
    """

    fields_needing_calibrations = (3, 6, 7)
    def normalize_fieldsums(self, fieldsums):
        cline = self.get_calibration(self.model, self.serial, self.dt)
        calibration = map(float, cline[5:11])
        for i in range(3):
            f = self.fields_needing_calibrations[i]
            if fieldsums[f][0]:
                fieldsums[f][1] /= fieldsums[f][0]
                fieldsums[f][1] = calibration[i*2 + 0] * (fieldsums[f][1] - calibration[i*2 + 1])
                fieldsums[f][0] = 1

    def Xadd_to_fieldsums(self, fieldsums, fields):
        """add the fields to the sums; logical-or the flags together."""
        flags = fields[5]
        fields[5] = "X"
        fields = Dataparser.add_to_fieldsums(self, fieldsums, fields)
        fieldsums[5][1] |= int(flags)
        fieldsums[5][0] = 1

    def set_dt_fields(self, fields):
        """ get the datetime, but also remember HMS """
        self.hms = fields[0]
        fields = Dataparser.set_dt_fields(self, fields)
        if len(fields) != 6:
            # see if it's a concatenated line
            line = ",".join(fields)
            line = line.split("OBS")
            if len(line) != 2: return []
            fields = line[1].split(",")
            if len(fields) != 9: return []
            fields = fields[3:9]
        # only use data with no flags set.
        if fields[5] != "000": return []
        red = "X"
        green = "X"
        blue = "X"
        if fields[2] == 'R': red = fields[3]
        elif fields[2] == 'G': green = fields[3]
        elif fields[2] == 'B': blue = fields[3]
        fields[3] = red
        fields.append(green)
        fields.append(blue)
        return fields

# base class for the LUMIC.
class Datalumic(Dataparser):

    def add_to_fieldsums(self, fieldsums, fields):
        """add the fields to the sums unless the error bits are non-zero"""
        if fields[3] != "000": return
        fields = Dataparser.add_to_fieldsums(self, fieldsums, fields)

    # mode,serial,counter,signal,reference,error bits

    def normalize_fieldsums(self, fieldsums):
        cline = self.get_calibration(self.model, self.serial, self.dt)
        calibration = [ float(re.sub(r' *^$', '0', v)) for v in cline[5:7] ]
        if fieldsums[1][0] and fieldsums[2][0]:
            fieldsums[1][1] /= fieldsums[1][0]
            fieldsums[2][1] /= fieldsums[2][0]
            fieldsums[1][0] = 1
            fieldsums[1][1] = calibration[0] * fieldsums[1][1]  / fieldsums[2][1] + calibration[1]

class Datalumic_accuracy(Datalumic):
    def dofiles(self, files, writer):
        writer.forcemethod("A")
        Dataparser.dofiles(self, files, writer)

class Datalumic_range(Datalumic):
    def dofiles(self, files, writer):
        writer.forcemethod("D")
        Dataparser.dofiles(self, files, writer)

 
class Datacdommc(Datalumic):
    pass

class Datachlamb(Datalumic):
    pass

class Dataobsbma(Datalumic_accuracy):
    pass

class Dataobsbmd(Datalumic_range):
    def add_to_fieldsums(self, fieldsums, fields):
        """add the fields to the sums unless the error bits are non-zero"""
        if fields[3] != "000" and fields[3] != "100": return
        fields = Dataparser.add_to_fieldsums(self, fieldsums, fields)

class Dataobsgma(Datalumic_accuracy):
    pass

class Dataobsgmd(Datalumic_range):
    def add_to_fieldsums(self, fieldsums, fields):
        """add the fields to the sums unless the error bits are non-zero"""
        if fields[3] != "000" and fields[3] != "100": return
        fields = Dataparser.add_to_fieldsums(self, fieldsums, fields)

class Dataobsrma(Datalumic_accuracy):
    pass

class Dataobsrmd(Datalumic_range):
    def add_to_fieldsums(self, fieldsums, fields):
        """add the fields to the sums unless the error bits are non-zero"""
        if fields[3] != "000" and fields[3] != "100": return
        fields = Dataparser.add_to_fieldsums(self, fieldsums, fields)


def searchfor(fn, filename):
    """ Read a different filename in the same or the parent folder as fn.
    >>> fn = os.tempnam()
    >>> fnt = os.path.join(fn,"t")
    >>> os.mkdir(fn)
    >>> open(fnt, "w").write("test")
    >>> searchfor(fn, "t")
    >>> searchfor(os.path.join(fn,"a"), "t")
    'test'
    >>> searchfor(os.path.join(fn,"a/a"), "t")
    'test'
    >>> searchfor(os.path.join(fn,"a/a/a"), "t")
    >>> os.unlink(fnt)
    >>> os.rmdir(fn)
    >>>
    """
    sitefn = os.path.join(os.path.dirname(fn), filename)
    if os.path.exists(sitefn):
        return open(sitefn).read().rstrip()
    else:
        sitefn = os.path.join(os.path.dirname(os.path.dirname(fn)), filename)
        if os.path.exists(sitefn):
            return open(sitefn).read().rstrip()
    return None

if __name__ == "__main__":
    import getopt
    import doctest

    rthssi = []
    for line in csv.reader(open("RTHS Sensor Inventory - Sheet1.csv")):
        rthssi.append(line)

    opts, args = getopt.getopt(sys.argv[1:], "c:prstuvd")
    if ("-t","") in opts:
        data = Dataparser(rthssi)
        writercsv = DatawriterCSV()
        writercsvone = DatawriterCSVone()
        doctest.testmod()
        sys.exit()

    for o,v in opts:
      if o == "-r":
        client = paramiko.SSHClient()
        client.load_system_host_keys()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect('ra-tes.org', 2739, 'pi')
        si,so,se = client.exec_command("tail --follow /service/logger/log/main/current")
        parsers = {}
        pbar = None
        for line in so:
            # @40000000534463aa25a08ba4 ttyACM1 ppal1,16,l,22076.16,48.67,12.50
            fields = line.split()
            if len(fields) != 3: continue
            # ppal1,16,l,22076.16,48.67,12.50
            fields = fields[2].split(",")
            if len(fields) < 3: continue
            model = fields.pop(0)
            serial = fields.pop(0)
            logtype = fields.pop(0)
            if model not in rths_sensors: continue
            if model not in parsers:
                if 'Data'+model in  locals():
                    parsers[model] = locals()['Data'+model](rthssi)
                else:
                    parsers[model] = Dataparser(rthssi)
                parsers[model].model = model
                parsers[model].serial = serial
                parsers[model].hms = None
                parsers[model].pbars= {}
                parsers[model].last_pbar = None
                
            fieldsums = parsers[model].zero_fieldsums()
            parsers[model].add_to_fieldsums(fieldsums, fields)
            parsers[model].normalize_fieldsums(fieldsums)
            if model.startswith('pbar'):
                if 'pdepth1' in parsers:
                    parsers['pdepth1'].last_pbar = ",".join(fields)
                if 'pdepth2' in parsers:
                    parsers['pdepth2'].last_pbar = ",".join(fields)

            for i, s in enumerate(rths_sensors[model]):
                if fieldsums[i][0]:
                    print s[1],fieldsums[i][1] / fieldsums[i][0]

    # sort the list of filenames by date
    files = sys.stdin.readlines()
    files.sort( lambda a,b: cmp(a.split('/')[-1], b.split('/')[-1]) )
    # change it into an array of filenames indexed by sensors
    filelistlist = []
    lastmodelserial = None
    for fn in files:
        site = os.path.basename(os.path.dirname(fn))
        fnmain = os.path.splitext(os.path.basename(fn))
        fnfields = fnmain[0].split('-')
        if fnfields[0] == 'voltage':
            modelserial = "%s-%s" % (fnfields[0], site)
        else:
            modelserial = "-".join(fnfields[1:3])
        if lastmodelserial != modelserial:
            filelistlist.append([])
            lastmodelserial = modelserial
        filelistlist[-1].append(fn)
    for filelist in filelistlist:

        fn = filelist[0]
        site = os.path.basename(os.path.dirname(fn))
        fnmain = os.path.splitext(os.path.basename(fn))
        fnfields = fnmain[0].split('-')
        if fnfields[0] == 'voltage':
            modelserial = "%s-%s" % (fnfields[0], site)
            model = fnfields[0].lower() 
        else:
            modelserial = "-".join(fnfields[1:3])
            model = fnfields[1].lower() 

        if ("-s","") in opts:
            config = "config.json"
            for o,v in opts:
                if o == '-c': config = v
            sitecode = searchfor(fn, ".sitecode")
            writer = DatawriterSQL(sitecode, json.load(open(config)))
            method = searchfor(fn, ".method-" + modelserial)
            if method: writer.forcemethod(method)
            if ("-d","") in opts:
                writer.deleting()
        elif ("-v","") in opts:
            writer = DatawriterView()
        elif ("-p","") in opts:
            writer = DatawriterCSVone()
        else:
            writer = DatawriterCSV()
        if 'Data'+model in  locals():
            data = locals()['Data'+model](rthssi)
        else:
            data = Dataparser(rthssi)

        data.dofiles(filelist, writer)
        writer.finish()

        # rename the files here, but only if there's a done folder to move them into.
        for fn in filelist:
            if fn.find("/done") >= 0: continue # already done, must be recapitulating
            fn = fn.rstrip()
            fnfields = os.path.split(fn)
            if os.path.exists(os.path.join( fnfields[0], "done")):
                fnnew = os.path.join( fnfields[0], "done", fnfields[1])
                os.rename(fn, fnnew)

# EOF

