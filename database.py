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
from sensors import rths_sites, rths_sensors
import datetime
import math
import pickle

class DatawriterCSV:
    """ provide methods for Dataparser to call to write data into a set of streams. """

    def __init__(self):
        pass

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

    def close(self):
        pass

class DatawriterCSVone(DatawriterCSV):
    """ like CSV, but one for one hour's worth of data, and passes the date through to the name """
    def _makefn(self, fn, serial, ymdh):
        """ Construct the destination filename
        >>> writercsvone._makefn("a", "b", "YYYYMMDD")
        'rths/YYYY/YYYYMM/a-b-YYYYMMDD.csv'
        >>> 
        """
        return "rths/%s/%s/" % (ymdh[0:4], ymdh[0:6]) + fn + "-%s-%s.csv" % (serial, ymdh)

class DatawriterSQL(DatawriterCSV):
    """ provide methods for Dataparser to call to write data into a set of streams. """


    def __init__(self, sitecode):
        DatawriterCSV.__init__(self)
        # using: ssh -n -N  -L 3307:localhost:3306 -i /root/.ssh/id_dsa mysqlfwd@www.ra-tes.org
        self.con = MySQLdb.connect(host='127.0.0.1', user='root', passwd='drjim1979', db='odm', port=3306)
        self.cur = self.con.cursor()
        self.sitecode = sitecode
        self.fieldcolumns = None
        self.SiteID = {}
        self.VariableID = {}
        self.MethodID = {}
        self.forcemethodname = None

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

    def write(self, dt, fieldsums):
        for i,fieldcolumn in enumerate(self.fieldcolumns):
            sitecode, variablecode, methoddescription = fieldcolumn[0:3]
            if variablecode and fieldsums[i][0]:
                self._insert(sitecode, variablecode, methoddescription, dt, fieldsums[i][1] / fieldsums[i][0])
                fieldcolumn[3] += 1
                fieldcolumn[4] = dt

    def close(self):
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
            except ValueError:
                print "one of these wasn't found:", SiteCode, VariableCode, MethodDescription
                raise
        return (self.SiteID[SiteCode], self.VariableID[VariableCode], self.MethodID[MethodDescription])

    
    def _insert(self, SiteCode, VariableCode, MethodDescription, dt, value):

        SiteID, VariableID, MethodID = self._get_site_variable(SiteCode, VariableCode, MethodDescription)

        UTCtime = dt.isoformat()
        tzoffset = -5 # DST can go to hell.
        dt += datetime.timedelta(hours=tzoffset)
        ESTtime = dt.isoformat()
        
        sqlcmd = """
            INSERT IGNORE INTO datavalues
            (SiteID,LocalDateTime,UTCOffset,DateTimeUTC,VariableID,
             DataValue,MethodID,SourceID,CensorCode)
            VALUES (%(SiteID)ld,'%(ESTtime)s',%(tzoffset)d,'%(UTCtime)s',%(VariableID)ld,
             %(value)f,%(MethodID)ld,1,'nc')
        ; """ % locals()

        return self.cur.execute(sqlcmd)

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
        """
        # log-MBIRDS-3-2011120423.gz
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
        >>> 
        """
        fnmain = os.path.splitext(os.path.basename(fn))
        fnfields = fnmain[0].split('-')
        self.YMD = fnfields[3][:8] # YYYYMMDD
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
        >>> data.parse_fn(fn)
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
        >>> data.set_dt_fields("00:59:58 0.00,0.00,19.71,33.02".split())
        ['0.00', '0.00', '19.71', '33.02']
        >>> data.dt
        datetime.datetime(2012, 11, 4, 4, 59, 58)
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
        self.dt = datetime.datetime(*time.strptime(self.YMD+fields[0], "%Y%m%d%H:%M:%S")[:6])
        if self.utc:
            tzoffset = 0; # wasn't that easy?
        elif self.dt < self.dst2012end: # DST 2012
            tzoffset = -4
        elif self.dt < self.dst2012endnext: # the last hour of DST 2012
            if self.dst is None or self.dt > self.dst:
                tzoffset = -4
                self.dst = self.dt # watch for wrap-around
            else:
                tzoffset = -5
        elif self.dt < self.dst2013begin: # ST 2012-2013
            tzoffset = -5
        elif self.dt < self.dst2013end: # DST 2013
            tzoffset = -4
        elif self.dt < self.dst2014begin: # ST 2013-2014
            tzoffset = -5
        else: # DST 2014-2014
            tzoffset = -4
        self.dt += datetime.timedelta(hours=-tzoffset)
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
        ['ph', '0', 'for St. Regis', '', '5/20/2013', '-0.8766', '31.99', '', '', '', '', '', '', '']
        >>> data.get_calibration("pH", "0", datetime.datetime(2013, 8, 31, 0,0,0))
        ['ph', '0', 'Grasse', '', '8/15/2013', '-1.0634', '37.28', '', '', '', '', '', '', '']
        >>> 
        """
        model = model.lower()
        for line in self.rthssi:
            if line[0] == model and line[1] == serial:
                if line[4] == "":
                    return line
                dt = self.date_parse(line[4])
                if dt < date: # only if it was calibrated before this measurement.
                    latest = line
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

                self.prepare_for(fn)
             
                for line in gzip.open(fn):
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
        # finish off any remaining samples
        try:
            self.normalize_fieldsums(fieldsums)
            writer.write(self.dt, fieldsums)
        except:
            print files 
            raise

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
        fieldsums[1][1] *= 100.0

class Datadepth2(Dataparser):
    def normalize_fieldsums(self, fieldsums):
        """ use centimeters, and adjust calibration on the low-range sensor. """
        fieldsums[0][1] /= fieldsums[0][0]
        fieldsums[0][1]  = fieldsums[0][1] / 3 + 0.94
        fieldsums[0][0]  = 1
        fieldsums[0][1] *= 100.0
        fieldsums[1][1] *= 100.0

class Datavoltage(Dataparser):
    """ read the battery voltage data. For historical reasons it's not in exactly the same file format. """

    def parse_first_fn(self, fn):
        """ we have to get the "serial" number from the filename """
        # /home/s8/voltage-2012-10-18.txt.gz
        # /home/s8/voltage-2013-05-04-13.txt.gz
        self.model = 'voltage'
        fnfields = fn.split('/')
        self.serial = fnfields[2] # actually the station name.
        dashfields = fnfields[3].split('-')
        self.YMDH = "".join(dashfields[1:4])
        if len(dashfields) == 5:
            # either way, YMDH is going to be unique
            self.YMDH += dashfields[4].split('.')[0]

    def parse_fn(self, fn):
        # every line in a voltage file has the YMD.
        pass

    def normalize_fieldsums(self, fieldsums):
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

class Datapdepth1(Dataparser):
    """ read the data produced by a pdepth1. We have to pull in pbar data to turn pressure into depth. """
    pcol = 4

    def prepare_for(self, fn):
        """ Given a pdepth1 or pdepth2 filename, find the associated pbar file and read it into a dict"""
        pfn = os.path.basename(fn)
        pfn = re.sub(r'pdepth[12]-.*-', 'pbar*-*-', pfn)
        for root, dirnames, filenames in os.walk(os.path.dirname(fn)):
            pbar_fns = fnmatch.filter(filenames, pfn)
            if len(pbar_fns) == 1: break
        else:
            raise "too many/few pbar_fns: %s %s %s" % ( fn, os.path.dirname(fn), pfn)
        self.pbars = {}
        for line in gzip.open(os.path.join(root, pbar_fns[0])):
            fields = line.split()
            if len(fields) != 2: continue
            if fields[1].startswith("reading"): continue
            self.pbars[fields[0]] = fields[1].rstrip()
        # get the first one, to make sure that we have one.
        ks = self.pbars.keys()
        ks.sort()
        if len(ks):
            self.last_pbar = self.pbars[ks[0]]

    def __init__(self, rthssi):
        """ initialize as usual. Also get calibrations out of Google docs. """
        Dataparser.__init__(self, rthssi)
        self.calibrations = {}
        for row in self.rthssi:
            self.calibrations[row[0]+'-'+row[1]] = row[6:12]

    def set_dt_fields(self, fields):
        """ get the datetime, but also remember HMS """
        self.hms = fields[0]
        return Dataparser.set_dt_fields(self, fields)

    def add_to_fieldsums(self, fieldsums, fields):
        # make room for the pbar sum.
        if len(fieldsums) != self.pcol + 1:
            fieldsums.append([0,0])
        Dataparser.add_to_fieldsums(self, fieldsums, fields[0:self.pcol])
        # pull pbar in.
        if self.hms in self.pbars:
            self.last_pbar = self.pbars[self.hms]
        pfields = self.last_pbar.split(',')
        try:
            b = float(pfields[1])
        except ValueError:
            pass
        else:
            fieldsums[self.pcol][0] += 1
            fieldsums[self.pcol][1] += b

    def normalize_fieldsums(self, fieldsums):
        # normalize
        for f in fieldsums:
            if f[0]:
                f[1] /= f[0]
                f[0] = 1
        # 5 psi temperature coefficient,5 psi load coefficient,15 psi temperature coefficient,15 psi load coefficient,offset
        calibration = map(float, self.calibrations[self.model + '-' + self.serial]) # crap out if it's not there.

        if calibration[0] == 0 and calibration[1] == 0 and calibration[4] == 0:
            # broken sensor - pretend it got no samples
            fieldsums[1][0] = 0
    
        if calibration[2] == 0 and calibration[3] == 0 and calibration[5] == 0:
            # broken sensor - pretend it got no samples
            fieldsums[2][0] = 0
    
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

class Datapdepth2(Datapdepth1):
    """ same data format, but we have btemperature in column 4. """
    pcol = 5


class Datacond(Dataparser):
    """ we come in with three columns, but need to output only one conductivity in uS/cm. """

    def __init__(self, rthssi):
        """ initialize as usual. Also get calibrations out of Google docs. """
        Dataparser.__init__(self, rthssi)
        self.calibrations = {}
        for row in self.rthssi:
            self.calibrations[row[0]+'-'+row[1]] = row[5:]

    def Xdoneaveraging(self):
        self.count += 1
        self.count %= 5
        return self.count == 0

    def normalize_fieldsums(self, fieldsums):
        calibration = map(float, self.calibrations[self.model.lower() + '-' + self.serial]) # crap out if it's not there.
        # get the average values
        for i in range(3):
            if fieldsums[i][0] == 0:
                # if any column has no samples, give up
                for i in range(3):
                    fieldsums[i][0] = 0
                return
        for i in range(3):
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
                    fieldsums[0][1] = calibration[6 + i] * (65535.0 / fieldsums[i][1] - 1)
                    fieldsums[0][0] = 1
                    break
        else:
            #print "found nothing in",fieldsums, 65535/4 , 65535 * 0.75
            pass
        # if none chosen, then we don't output any value (outside of calibrated ranges).

class Datafl3(Dataparser):
    def normalize_fieldsums(self, fieldsums):
        cline = self.get_calibration(self.model, self.serial, self.dt)
        calibration = map(float, cline[5:11])
        for i in range(2):
            col = [1,5][i]
            if fieldsums[col][0]:
                fieldsums[col][1] /= fieldsums[0][0]
                fieldsums[col][1] = calibration[0 + 3 *i] * (fieldsums[col][1] - calibration[2 + 3 *i]) + calibration[1 + 3 *i]
                fieldsums[col][0] = 1

class Dataph(Dataparser):
    def normalize_fieldsums(self, fieldsums):
        cline = self.get_calibration(self.model, self.serial, self.dt)
        calibration = map(float, cline[5:7])
        if fieldsums[0][0]:
            fieldsums[0][1] /= fieldsums[0][0]
            fieldsums[0][1] = calibration[0] * fieldsums[0][1] + calibration[1]
            fieldsums[0][0] = 1

class Datado3(Dataparser):
    def normalize_fieldsums(self, fieldsums):
        cline = self.get_calibration(self.model, self.serial, self.dt)
        calibration = map(float, cline[5:7])
        if fieldsums[0][0]:
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
        if self.load() and self.dt + datetime.timedelta(hours = 1, minutes=1) >= dt:
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
        self.calibrations = {}
        for row in self.rthssi:
            self.calibrations[row[0]+'-'+row[1]] = row[5]

    def dofiles(self, files, writer):
        Dataparser.dofiles(self, files, writer)
        self.h.dump()

    def doneaveraging(self):
        """ average one hour worth of samples. """
        if False:
            done = self.minuteperiod is not None and self.dt.minute / 15 != self.minuteperiod
            self.minuteperiod = self.dt.minute / 15
        else:
            done = self.minuteperiod is not None and self.dt.hour != self.minuteperiod
            self.minuteperiod = self.dt.hour
        return done

    def normalize_fieldsums(self, fieldsums):
        if self.h is None:
            self.h = History(self.model + "-" + self.serial, self.dt)
        if fieldsums[0][0] == 0: return
        # only output deltas if the start of deltas exceeded threshold
        cal = float(self.calibrations[self.model + '-' + self.serial])
        ave = float(fieldsums[0][1]) / fieldsums[0][0]
        this = ave  / cal
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

class Datappal1(Datappal):
    """ same data format """

class Datappal2(Datappal):
    """ same data format """

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

    def __init__(self, rthssi):
        Dataparser.__init__(self, rthssi)
        self.calibrations = {}
        for row in self.rthssi:
            self.calibrations[row[0]+'-'+row[1]] = row[5:11]

    fields_needing_calibrations = (3, 6, 7)
    def normalize_fieldsums(self, fieldsums):
        calibration = map(float, self.calibrations[self.model.lower() + '-' + self.serial]) # crap out if it's not there.
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

def searchfor(fn, filename):
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

    opts, args = getopt.getopt(sys.argv[1:], "tpsu")
    if ("-t","") in opts:
        data = Dataparser(rthssi)
        writercsv = DatawriterCSV()
        writercsvone = DatawriterCSVone()
        doctest.testmod()
        sys.exit()

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
            sitecode = searchfor(fn, ".sitecode")
            writer = DatawriterSQL(sitecode)
            method = searchfor(fn, ".method-" + modelserial)
            if method: writer.forcemethod(method)
        elif ("-p","") in opts:
            writer = DatawriterCSVone()
        else:
            writer = DatawriterCSV()
        if 'Data'+model in  locals():
            data = locals()['Data'+model](rthssi)
        else:
            data = Dataparser(rthssi)

        data.dofiles(filelist, writer)
        writer.close()
        # rename the files here.
        for fn in filelist:
            if fn.find("/done") >= 0: continue # already done, must be recapitulating
            fn = fn.rstrip()
            fnfields = os.path.split(fn)
            fnnew = os.path.join( fnfields[0], "done", fnfields[1])
            os.rename(fn, fnnew)

# EOF
