#!/usr/bin/python
# vim: set ai sw=4 sta fo=croql ts=8 expandtab syntax=python
                                                                               # die, PEP8's 80-column punched card requirement!

# DST was at 2AM on November 4th, 2012

import time
import sys
import gzip
import os
import csv
import re
import fnmatch
import MySQLdb
from all import rths_sites
import datetime

class DatawriterCSV:
    """ provide methods for Dataparser to call to write data into a set of streams. """

    def __init__(self):
        pass

    def open(self, fields, model, serial):
        self.fieldfiles = {}
        for fn,fieldnum,values in fields:
            outf = open(fn + "-%s.csv" % serial, "w")
            outf.write(values)
            self.fieldfiles[fieldnum] = outf

    def write(self, dt, fieldsums):
        timestamp = time.strftime("%m/%d/%Y %H:%M:%S", dt)
        for fieldnum,outf in self.fieldfiles.items():
            if fieldsums[fieldnum][0]:
                outf.write("%s,%.3f\n" % (timestamp, fieldsums[fieldnum][1] / fieldsums[fieldnum][0]))


class DatawriterSQL(DatawriterCSV):
    """ provide methods for Dataparser to call to write data into a set of streams. """


    def __init__(self):
        DatawriterCSV.__init__(self)
        # using: ssh -n -N  -L 3307:localhost:3306 -i /root/.ssh/id_dsa mysqlfwd@www.ra-tes.org
        self.con = MySQLdb.connect(host='127.0.0.1', user='odbinsert', passwd='bn8V9!rL', db='odm', port=3307)
        self.cur = self.con.cursor()

    def model_serial(self, model, serial):
        """Get the id, name, and county for this model & serial"""
        for name, sensors in rths_sites:
            (id, name, county, techno) = name
            for sensor in sensors:
                m,s= sensor.split("-")
                if model == m and serial == s:
                    return (id, name, county)

    def open(self, fields, model, serial):
        self.fieldcolumns = []
        for fn,fieldnum,values in fields:
            name = self.model_serial(model, serial)[1].replace(" ", "")
            self.fieldcolumns.append([fn, name])

    def write(self, dt, fieldsums):
        for i,f in enumerate(self.fieldcolumns):
            variable, site = f
            if fieldsums[i][0]:
                self.insert(site, variable, dt, fieldsums[i][1] / fieldsums[i][0])

    def insert(self, SiteCode, VariableCode, localtime, value):
        ESTtime = localtime.isoformat()
        UTCtime = localtime.utctimetuple().isoformat()
        #return self.cur.execute
        print ("insert into DataValues (SiteID,LocalDateTime,UTCOffset,DateTimeUTC,VariableID,DataValue,MethodID,SourceID,CensorCode)" +
        "( select Sites.SiteID,'%s',-5,'%s',Variables.VariableID,%s,Methods.MethodID,1,'nc' " +
        "from Sites, Variables, Methods " +
        "where Sites.SiteCode = '%s' and Variables.VariableCode = '%s' and Methods.MethodDescription = 'Autonomous Sensing');" % 
        (value, ESTtime, UTCTime, SiteCode, VariableCode))


class Dataparser:

    def readfilenames(self, inf):
        self.files = inf.readlines()
        self.files.sort( lambda a,b: cmp(a.split('/')[-1], b.split('/')[-1]) )

    # map sensor model name into the filenames, fields, and CSV column titles.
    fieldnames = { "windair": (
                 ("windspeed", 0, "Date,Wind Speed\n"),
                 ("winddir", 1, "Date,Wind Direction\n"),
                 ("airtemp", 2, "Date,Temperature\n"),
                 ("humidity", 3, "Date,Relative Humidity\n")
                ),
                "ppal": (
                 ("bigdepth", 0, "Date,Depth\n"),
                 ("littledepth", 1, "Date,Depth\n"),
                 ("temperature", 2, "Date,Temperature\n")
                ),
                "ppal1": (
                 ("bigdepth", 0, "Date,Depth\n"),
                 ("littledepth", 1, "Date,Depth\n"),
                 ("temperature", 2, "Date,Temperature\n")
                ),
                "voltage": (
                 ("voltage", 0, "Date,Voltage\n"),
                ),
                "pdepth": (
                 ("littledepth", 0, "Date,Depth\n"),
                 ("temperature", 1, "Date,Temperature\n")
                ),
                "pdepth1": (
                 ("something", 0, "Date,Something\n"),
                 ("littledepth", 1, "Date,Depth\n"),
                 ("bigdepth", 2, "Date,Depth\n"),
                 ("temperature", 3, "Date,Temperature\n")
                ),
            }

    def parse_first_fn(self, fn):
        """ remember things taken from the first filename """
        # log-MBIRDS-3-2011120423.gz
        fnmain = os.path.splitext(os.path.basename(fn))
        fnfields = fnmain[0].split('-')
        self.model = fnfields[1]
        self.serial = fnfields[2]
        self.YMD = fnfields[3][:-2] # YYYYMMDD
 
    def set_dt_fields(self, fields):
        """ remember the full datetime for each line, and return the data fields. """
        # 10:06:19 4.59,999.00,257
        self.dt = time.strptime(self.YMD+fields[0], "%Y%m%d%H:%M:%S")
        return fields[1].split(',')

    def normalize_fieldsums(self, fieldsums):
        """ normalize the sums as needed for calibration constants, etc """
        pass

    def __init__(self):
        pass

    def prepare_for(self, fn):
        """ do whatever is necessary to prepare for accessing this file."""
        pass

    def add_to_fieldsums(self, fieldsums, fields):
        """add the fields to the sums."""
        for i in range(len(fields)):
            fieldsums[i][0] += 1
            fieldsums[i][1] += float(fields[i])

    def dofiles(self, writer):
        """ read a list of files, create running averages and split out by sensor."""
        self.readfilenames(sys.stdin)

        lazy_opened = False

        self.dt = None # keep track of the latest timestamp found.
        count = 0
        fieldsums = []
        for fn in self.files:
            try:
                fn = fn.rstrip()
                if not lazy_opened:
                    self.parse_first_fn(fn)
                    writer.open(self.fieldnames[self.model], self.model, self.serial)
                    fieldsums = [[0 for col in range(2)] for row in range(len(self.fieldnames[self.model]))]
                    lazy_opened = True

                # get the YMD of this file.
                fnmain = os.path.splitext(os.path.basename(fn))
                fnfields = fnmain[0].split('-')
                self.YMD = fnfields[3][:-2] # YYYYMMDD

                self.prepare_for(fn)
             
                for line in gzip.open(fn):
                    fields = self.set_dt_fields(line.split())
                    if len(fields) != len(self.fieldnames[self.model]): continue
                    self.add_to_fieldsums(fieldsums, fields)
                    count += 1
                    if count > 20:
                        self.normalize_fieldsums(fieldsums)
                        writer.write(self.dt, fieldsums)
                        fieldsums = [[0 for col in range(2)] for row in range(len(self.fieldnames[self.model]))]
                        count = 0
            except:
                print fn
                raise

        if self.dt is not None:
            # remember the most recently found timestamp in a file.
            lastrx = time.mktime(self.dt)
            fn = "timestamp-%s-%s" % (self.model, self.serial)
            open(fn, "w")
            os.utime(fn, (lastrx, lastrx))


class Datavoltage(Dataparser):
    """ read the battery voltage data. For historical reasons it's not in exactly the same file format. """

    def parse_first_fn(self, fn):
        """ we have to get the "serial" number from the filename """
        # /home/s8/voltage-2012-10-18.txt.gz
        self.model = 'voltage'
        self.serial = fn.split('/')[2] # actually the station name.

    def normalize_fieldsums(self, fieldsums):
        """ map the value from an A/D value into a voltage"""
        fieldsums[0][1] *= 7.25

    def set_dt_fields(self, fields):
        """ parse a ctime timestamp and return the voltage field """
        t = " ".join(fields[1:5])
        self.dt = time.strptime(t, "%b %d %H:%M:%S %Y")
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
        if self.lastavgt is None:
            fieldsums[1][0] = 0
        if fieldsums[1][0]:
            # in case we get a run of bad samples, don't throw off the average.
            self.lastavgt = fieldsums[1][1] / fieldsums[1][0]
        if fieldsums[0][0]:
            # average and convert counts to centimeters.
            fieldsums[0][1] /= fieldsums[0][0]
            fieldsums[0][0] = 1
            inches = 0.1328 * fieldsums[0][1] + 1.1033
            fieldsums[0][1] = inches * 2.54

class Datapdepth1(Dataparser):
    """ read the data produced by a pdepth1. We have to pull in pbar data to turn pressure into depth. """

    def prepare_for(self, fn):
        """ Given a pdepth1 filename, find the associated pbar file and read it into a dict"""
        pfn = os.path.basename(fn)
        pfn = re.sub(r'pdepth1-.*-', 'pbar*-*-', pfn)
        for root, dirnames, filenames in os.walk(os.path.dirname(fn)):
            pbar_fns = fnmatch.filter(filenames, pfn)
            if len(pbar_fns) == 1: break
        else:
            raise "too many/few pbar_fns"
        self.pbars = {}
        for line in gzip.open(os.path.join(root, pbar_fns[0])):
            fields = line.split()
            self.pbars[fields[0]] = fields[1].rstrip()
        # get the first one, to make sure that we have one.
        ks = self.pbars.keys()
        ks.sort()
        self.last_pbar = self.pbars[ks[0]]

    def __init__(self):
        """ initialize as usual. Also get calibrations out of Google docs. """
        Dataparser.__init__(self)
        self.calibrations = {}
        rthssi = csv.reader(open("RTHS Sensor Inventory - Sheet1.csv"))
        for row in rthssi:
            self.calibrations[row[0]+'-'+row[1]] = row[6:]

    def set_dt_fields(self, fields):
        """ get the datetime, but also remember HMS """
        self.hms = fields[0]
        return Dataparser.set_dt_fields(self, fields)


    def add_to_fieldsums(self, fieldsums, fields):
        # make room for the pbar sum.
        if len(fieldsums) != 5:
            fieldsums.append([0,0])
        Dataparser.add_to_fieldsums(self, fieldsums, fields)
        # pull pbar in.
        if self.hms in self.pbars:
            self.last_pbar = self.pbars[self.hms]
        pfields = self.last_pbar.split(',')
        fieldsums[4][0] += 1
        fieldsums[4][1] += float(pfields[1])

    def normalize_fieldsums(self, fieldsums):
        # normalize
        for f in fieldsums:
            f[1] /= f[0]
            f[0] = 1
        # 5 psi temperature coefficient,5 psi load coefficient,15 psi temperature coefficient,15 psi load coefficient,offset
        calibration = map(float, self.calibrations[self.model + '-' + self.serial]) # crap out if it's not there.
    
        baro = fieldsums[4][1]
        temp = fieldsums[3][1]
        fieldsums[1][1] = (fieldsums[1][1]+ ( 1013.25 - baro ) * 0.402 * calibration[1] + (temp - 35) * calibration[0]) / calibration[1] + calibration[4]
        fieldsums[2][1] = (fieldsums[2][1]+ ( 1013.25 - baro ) * 0.402 * calibration[3] + (temp - 35) * calibration[2]) / calibration[3] + calibration[4]
        fieldsums[1][1] *= 2.54
        fieldsums[2][1] *= 2.54
        fieldsums[0][0] = 0 # ignore first column


if __name__ == "__main__":
    import getopt

    opts, args = getopt.getopt(sys.argv[1:], "su")
    if ("-s","") in opts:
        writer = DatawriterSQL()
    else:
        writer = DatawriterCSV()

    if ("-u","") in opts:
        update()
    elif len(args) == 0: 
        data = Dataparser()
    elif args[0] == 'voltage':
        data = Datavoltage()
    elif args[0] == 'pdepth':
        data = Datapdepth()
    elif args[0] == 'pdepth1':
        data = Datapdepth1()
    data.dofiles(writer)

# EOF

"""
%Jimmy's Algorithm
for i=2:length(delta)
    if firsttime
        rain=0;
        lastrain=praw{1,2}(i);
        firsttime=0;
    end
    if delta(i) > x
        rain=1;
    end
    if rain
        if praw{1,2}(i)>=praw{1,2}(i-1)
            precip(i,1)=praw{1,2}(i);%accumulated precip
            iprecip(i,1) = praw{1,2}(i)-praw{1,2}(i-1);%instantaneous precip
            event(n,3) = event(n,3)+iprecip(i,1);
            if(firstrain)
                event(n,1)=prawdate(i,1);
                firstrain=0;
            end
        else
            event(n,2)=prawdate(i,1);
            n=n+1;
            precip(i,1)=praw{1,2}(i);
            rain=0;
            firstrain=1;
            lastrain=precip(i,1);
            iprecip(i,1)=0;
            firstdown=1;
        end
    else
        precip(i,1)=lastrain;
        iprecip(i,1)=0; %put this in for fall, might also need for winter
    end
end
""" 
