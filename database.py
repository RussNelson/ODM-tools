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

class DatawriterCSV:
    """ provide methods for Dataparser to call to write data into a set of streams. """

    def __init__(self):
        pass

    def _makefn(self, fn, serial, ymdh):
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
       return "rths/%s/%s/" % (ymdh[0:4], ymdh[0:6]) + fn + "-%s-%s.csv" % (serial, ymdh)

class DatawriterSQL(DatawriterCSV):
    """ provide methods for Dataparser to call to write data into a set of streams. """


    def __init__(self, sitecode):
        DatawriterCSV.__init__(self)
        # using: ssh -n -N  -L 3307:localhost:3306 -i /root/.ssh/id_dsa mysqlfwd@www.ra-tes.org
        self.con = MySQLdb.connect(host='127.0.0.1', user='odbinsert', passwd='bn8V9!rL', db='odm', port=3307)
        self.cur = self.con.cursor()
        self.sitecode = sitecode
        self.fieldcolumns = None

    def open(self, fields, model, serial, ymdh):
        self.fieldcolumns = []
        for fieldnum,field in enumerate(fields):
            variable = field[1] # rths_sensors
            self.fieldcolumns.append([self.sitecode, variable, 0, None])

    def write(self, dt, fieldsums):
        for i,fieldcolumn in enumerate(self.fieldcolumns):
            site, variable = fieldcolumn[0:2]
            if fieldsums[i][0]:
                self.insert(site, variable, dt, fieldsums[i][1] / fieldsums[i][0])
                fieldcolumn[2] += 1
                fieldcolumn[3] = dt

    def close(self):
        if self.fieldcolumns is None: return
        for i,fieldcolumn in enumerate(self.fieldcolumns):
            site, variable, count, dt = fieldcolumn
            if count:
                print site,variable,dt,count,self.update(site, variable, dt, count)
        self.con.commit()

    updatesql = """update seriescatalog
        set EndDateTime = '%s', EndDateTimeUTC = '%s',
            ValueCount = ValueCount + %s
        where SiteCode = '%s' and VariableCode = '%s'
        ; """.replace("\n", "")

    def update(self, SiteCode, VariableCode, dt, count):
        UTCtime = dt.isoformat()
        tzoffset = -5 # DST can go to hell.
        dt += datetime.timedelta(hours=tzoffset)
        ESTtime = dt.isoformat()
        sqlcmd = self.updatesql % (ESTtime, UTCtime,
            count, SiteCode, VariableCode)

        print sqlcmd
        return self.cur.execute(sqlcmd)
    
    insertsql = """insert into datavalues
        (SiteID,LocalDateTime,UTCOffset,DateTimeUTC,VariableID,
         DataValue,MethodID,SourceID,CensorCode)
        ( select sites.SiteID,'%s',%d,'%s',variables.VariableID,
         '%s',methods.MethodID,1,'nc' 
        from sites, variables, methods 
        where sites.SiteCode = '%s'
          and variables.VariableCode = '%s'
          and methods.MethodDescription = 'Autonomous Sensing')
        ; """.replace("\n","")

    def insert(self, SiteCode, VariableCode, dt, value):
        UTCtime = dt.isoformat()
        tzoffset = -5 # DST can go to hell.
        dt += datetime.timedelta(hours=tzoffset)
        ESTtime = dt.isoformat()
        sqlcmd =  self.insertsql % (ESTtime, tzoffset, UTCtime,
            value, SiteCode, VariableCode)

        return self.cur.execute(sqlcmd)


class Dataparser:

    def __init__(self):
        self.count = 0
        self.minuteperiod = None

    def parse_first_fn(self, fn):
        """ remember things taken from the first filename """
        # log-MBIRDS-3-2011120423.gz
        fnmain = os.path.splitext(os.path.basename(fn))
        fnfields = fnmain[0].split('-')
        self.model = fnfields[1]
        self.serial = fnfields[2]
        self.YMDH = fnfields[3] # YYYYMMDDHH

    def parse_fn(self, fn):
        fnmain = os.path.splitext(os.path.basename(fn))
        fnfields = fnmain[0].split('-')
        self.YMD = fnfields[3][:8] # YYYYMMDD
        self.dst = None
 
    dst2012end = datetime.datetime(2012, 11, 4, 1) # actually at 2AM, but it all goes into the 1AM file.
    dst2012endnext = datetime.datetime(2012, 11, 4, 2)
    dst2013begin = datetime.datetime(2013, 3, 10, 2)
    dst2013end = datetime.datetime(2013, 11, 3, 2)

    def set_dt_fields(self, fields):
        """ remember the full UTC datetime for each line, and return the data fields. """
        # the data files were recorded in Eastern time, complete with DST crap.
        # each line is of the form: 10:06:19 4.59,999.00,257
        # we get it pre-split into two fields at the space.
        self.dt = datetime.datetime(*time.strptime(self.YMD+fields[0], "%Y%m%d%H:%M:%S")[:6])
        if self.dt < self.dst2012end: # DST 2012
            tzoffset = -4
        elif self.dt < self.dst2012endnext: # the last hour of DST 2012
            if self.dst is None or self.dt > self.dst:
                tzoffset = -4
                self.dst = self.dt # watch for wrap-around
            else:
                tzoffset = -5
        elif self.dt < self.dst2013begin: # ST 2012-2013
            tzoffset = -5
        else: # DST 2013
            tzoffset = -4
        self.dt += datetime.timedelta(hours=-tzoffset)
        return fields[1].split(',')

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

    def dofiles(self, files, writer, rthssi):
        """ given a list of files, create running averages and split out by sensor."""
        self.rthssi = rthssi

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
                self.parse_fn(fn)

                self.prepare_for(fn)
             
                for line in gzip.open(fn):
                    fields = line.split()
                    if len(fields) < 2: continue
                    if fields[0] == "starting": continue
                    fields = self.set_dt_fields(fields)
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
        self.normalize_fieldsums(fieldsums)
        writer.write(self.dt, fieldsums)

        if self.dt is not None:
            # remember the most recently found timestamp in a file.
            lastrx = time.mktime(self.dt.timetuple()) 
            fn = "timestamp-%s-%s" % (self.model, self.serial)
            open(fn, "w")
            os.utime(fn, (lastrx, lastrx))

class Datadepth2(Dataparser):
    def normalize_fieldsums(self, fieldsums):
        """ adjust calibration on the low-range sensor. """
        fieldsums[0][1] /= fieldsums[0][0]
        fieldsums[0][1]  = fieldsums[0][1] / 3 + 0.94
        fieldsums[0][0]  = 1

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
        """ map the value from an A/D value into a voltage"""
        fieldsums[0][1] *= 7.25

    def set_dt_fields(self, fields):
        """ parse a ctime timestamp and return the voltage field """
        t = " ".join(fields[1:5])
        then = time.strptime(t, "%b %d %H:%M:%S %Y")
        self.dt = datetime.datetime.fromtimestamp(time.mktime(then))
        return fields[5:6]

class Datapdepth(Dataparser):
    """ read the data produced by a pdepth. We have to throw out bad temperature samples. """
    def __init__(self):
        Dataparser.__init__(self)
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

    def prepare_for(self, fn):
        """ Given a pdepth1 or pdepth2 filename, find the associated pbar file and read it into a dict"""
        pfn = os.path.basename(fn)
        pfn = re.sub(r'pdepth[12]-.*-', 'pbar*-*-', pfn)
        for root, dirnames, filenames in os.walk(os.path.dirname(fn)):
            pbar_fns = fnmatch.filter(filenames, pfn)
            if len(pbar_fns) == 1: break
        else:
            raise "too many/few pbar_fns"
        self.pbars = {}
        for line in gzip.open(os.path.join(root, pbar_fns[0])):
            fields = line.split()
            if fields[1].startswith("reading"): continue
            self.pbars[fields[0]] = fields[1].rstrip()
        # get the first one, to make sure that we have one.
        ks = self.pbars.keys()
        ks.sort()
        self.last_pbar = self.pbars[ks[0]]

    def __init__(self):
        """ initialize as usual. Also get calibrations out of Google docs. """
        Dataparser.__init__(self)
        self.calibrations = {}
        rthssi = csv.reader(open("/var/www/ra-tes.org/RTHS Sensor Inventory - Sheet1.csv"))
        for row in rthssi:
            self.calibrations[row[0]+'-'+row[1]] = row[6:12]

    def set_dt_fields(self, fields):
        """ get the datetime, but also remember HMS """
        self.hms = fields[0]
        return Dataparser.set_dt_fields(self, fields)

    def add_to_fieldsums(self, fieldsums, fields):
        # make room for the pbar sum.
        if len(fieldsums) != 5:
            fieldsums.append([0,0])
        Dataparser.add_to_fieldsums(self, fieldsums, fields[0:4])
        # pull pbar in.
        if self.hms in self.pbars:
            self.last_pbar = self.pbars[self.hms]
        pfields = self.last_pbar.split(',')
        try:
            b = float(pfields[1])
        except ValueError:
            pass
        else:
            fieldsums[4][0] += 1
            fieldsums[4][1] += b

    def normalize_fieldsums(self, fieldsums):
        # normalize
        for f in fieldsums:
            if f[0]:
                f[1] /= f[0]
                f[0] = 1
        # 5 psi temperature coefficient,5 psi load coefficient,15 psi temperature coefficient,15 psi load coefficient,offset
        calibration = map(float, self.calibrations[self.model + '-' + self.serial]) # crap out if it's not there.
    
        baro = fieldsums[4][1]
        temp = fieldsums[3][1]
        if fieldsums[1][0]:
            fieldsums[1][1] = (fieldsums[1][1]+ ( 1013.25 - baro ) * 0.402 * calibration[1] + (temp - 35) * calibration[0]) / calibration[1] + calibration[4]
            fieldsums[1][1] *= 2.54
        if fieldsums[2][0]:
            fieldsums[2][1] = (fieldsums[2][1]+ ( 1013.25 - baro ) * 0.402 * calibration[3] + (temp - 35) * calibration[2]) / calibration[3] + calibration[5]
            fieldsums[2][1] *= 2.54
        fieldsums[0][0] = 0 # ignore first column

class Datapdepth2(Datapdepth1):
    """ same data format """

class Datacond(Dataparser):
    """ we come in with three columns, but need to output only one conductivity in uS/cm. """

    def __init__(self):
        """ initialize as usual. Also get calibrations out of Google docs. """
        Dataparser.__init__(self)
        self.calibrations = {}
        rthssi = csv.reader(open("/var/www/ra-tes.org/RTHS Sensor Inventory - Sheet1.csv"))
        for row in rthssi:
            self.calibrations[row[0]+'-'+row[1]] = row[5:11]

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
                if ((i == 0 and fieldsums[i][1] < 65535 * 0.90) or
                    (i == 1 and 65535/4 < fieldsums[i][1] < 65535 * 0.90) or
                    (i == 2 and 65535/4 < fieldsums[i][1])):
                    fieldsums[0][1] = calibration[i*2] * math.exp( calibration[i*2+1] * fieldsums[i][1])
                    fieldsums[0][0] = 1
                    break
        else:
            #print "found nothing in",fieldsums, 65535/4 , 65535 * 0.75
            pass
        # if none chosen, then we don't output any value (outside of calibrated ranges).

class Dataph(Dataparser):
    def __init__(self):
        """ initialize as usual. Also get calibrations out of Google docs. """
        Dataparser.__init__(self)
        self.calibrations = {}
        rthssi = csv.reader(open("/var/www/ra-tes.org/RTHS Sensor Inventory - Sheet1.csv"))
        for row in rthssi:
            self.calibrations[row[0]+'-'+row[1]] = row[5:7]

    def normalize_fieldsums(self, fieldsums):
        calibration = map(float, self.calibrations[self.model.lower() + '-' + self.serial]) # crap out if it's not there.
        if fieldsums[0][0]:
            fieldsums[0][1] /= fieldsums[0][0]
            fieldsums[0][1] = calibration[0] * fieldsums[0][1] + calibration[1]
            fieldsums[0][0] = 1

class Datappal(Dataparser):

    threshold = 0.00975
    threshold = 0.0087 # rain period starts with a delta at least this big.
    threshold = 0.039  # per Jimmy 6/21/213

    def __init__(self):
        Dataparser.__init__(self)
        self.minuteperiod = None
        self.raining = False ### how to carry this over from analysis period to analysis period??
        self.previous = None
        self.calibrations = {}
        rthssi = csv.reader(open("/var/www/ra-tes.org/RTHS Sensor Inventory - Sheet1.csv"))
        for row in rthssi:
            self.calibrations[row[0]+'-'+row[1]] = row[5]

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
        # only output deltas if the start of deltas exceeded threshold
        cal = float(self.calibrations[self.model + '-' + self.serial])
        ave = float(fieldsums[0][1]) / fieldsums[0][0]
        this = ave  / cal
        if self.previous is None: # remember the first (but we should be carrying over from previous)
            self.previous = this
            fieldsums[0][0] = 0
            return
        delta = this - self.previous
        self.previous = this
        if delta > self.threshold:
            self.raining = True
        if self.raining and delta > 0:
            fieldsums[0][1] = delta * 2.54 # convert inches to cm
        else:
            self.raining = False
            fieldsums[0][1] = 0
        fieldsums[0][0] = 1

class Datappal1(Datappal):
    """ same data format """

class Datappal2(Datappal):
    """ same data format """

class Dataobs(Dataparser):

    """07:00:26 0,485820,B,151.03,OBS,8,L,0,485821,R,81.96,271.22,000
    07:00:28 0,485822,G,93.80,3OBS,8,L,0,485823,B,151.21,405.59,000
    07:00:30 0,485824,R,77.60,OBS,8,L,0,485825,G,87.94,328.07,000
    07:00:32 0,485826,B,146.13,405.63,000
    07:00:32 0,485827,R,80.71,271.22,000
    07:00:33 0,485828,G,92.20,328.07,000
    """

    def __init__(self):
        Dataparser.__init__(self)
        self.calibrations = {}
        rthssi = csv.reader(open("/var/www/ra-tes.org/RTHS Sensor Inventory - Sheet1.csv"))
        for row in rthssi:
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

if __name__ == "__main__":
    import getopt
    import doctest

    inf = csv.reader(open("/var/www/ra-tes.org/RTHS Sensor Inventory - Sheet1.csv"))
    rthssi = []
    for row in inf:
        rthssi.append(row)

    opts, args = getopt.getopt(sys.argv[1:], "tpsu")
    if ("-t","") in opts:
        doctest.testmod()
        sys.exit()

    # sort the list of filenames by date
    files = sys.stdin.readlines()
    files.sort( lambda a,b: cmp(a.split('/')[-1], b.split('/')[-1]) )
    # change it into an array of sensors
    filelistlist = []
    lastmodelserial = None
    for file in files:
        fnmain = os.path.splitext(os.path.basename(file))
        fnfields = fnmain[0].split('-')
        modelserial = "-".join(fnfields[1:3])
        if lastmodelserial != modelserial:
            filelistlist.append([])
            lastmodelserial = modelserial
        filelistlist[-1].append(file)
    for filelist in filelistlist:

        fn = filelist[0]
        fnmain = os.path.splitext(os.path.basename(fn))
        fnfields = fnmain[0].split('-')
        model = fnfields[1].lower() 
        site = os.path.basename(os.path.dirname(fn))
        sitecode = open(os.path.join(os.path.dirname(fn), ".sitecode")).read()

        if ("-s","") in opts:
            writer = DatawriterSQL(sitecode)
        elif ("-p","") in opts:
            writer = DatawriterCSVone()
        else:
            writer = DatawriterCSV()
        if 'Data'+model in  locals():
            data = locals()['Data'+model]()
        else:
            data = Dataparser()

        data.dofiles(filelist, writer, rthssi)
        writer.close()
        # rename the files here.


# EOF

