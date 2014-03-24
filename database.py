#!/usr/bin/python
                                                                               # die, PEP8's 80-column punched card requirement!

# DST was at 2AM on November 4th, 2012

import time
import sys
import gzip
import os

class DatawriterCSV:
    """ provide methods for Dataparser to call to write data into a set of streams. """
    def open(self, fields, serial):
        self.fieldfiles = {}
	for f,fieldnum,values in fields:
	    outf = open(f + "-%s.csv" % serial, "w")
	    outf.write(values)
	    self.fieldfiles[fieldnum] = outf
    def write(self, dt, fieldsums):
	timestamp = time.strftime("%m/%d/%Y %H:%M:%S", dt)
	for fieldnum,outf in self.fieldfiles.items():
	    if fieldsums[fieldnum][0]:
		outf.write("%s,%.3f\n" % (timestamp, fieldsums[fieldnum][1] / fieldsums[fieldnum][0]))

class Dataparser:

    def readfilenames(self, inf):
        self.files = inf.readlines()
        self.files.sort( lambda a,b: cmp(a.split('/')[-1], b.split('/')[-1]) )

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

    def add_to_fieldsums(self, fieldsums, fields):
        for i in range(len(fieldsums)):
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
		    writer.open(self.fieldnames[self.model], self.serial)
                    fieldsums = [[0,0]] * len(self.fieldnames[self.model])
                    lazy_opened = True
             
                for line in gzip.open(fn):
                    fields = self.set_dt_fields(line.split())
                    if len(fields) != len(self.fieldnames[self.model]): continue
                    self.add_to_fieldsums(fieldsums, fields)
                    for i in range(len(fieldsums)):
                        fieldsums[i][0] += 1
                        fieldsums[i][1] += float(fields[i])
		    count += 1
                    if count > 20:
                        self.normalize_fieldsums(fieldsums)
			writer.write(self.dt, fieldsums)
                        fieldsums = [[0,0]] * len(self.fieldnames[self.model])
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
        Dataparser.__init__()
        self.lastavgt = None

    def add_to_fieldsums(self, fieldsums, fields):
        # add the depth.
        fieldsums[0][0] += 1
        fieldsums[0][1] += float(fields[0])
	# throw out temperature samples which are more than +/- 5% different than last average.
	temp = float(fields[1])
	if self.lastavgt is None or self.lastavgt * 0.95 < temp < self.lastavgt * 1.05:
            fieldsums[1][0] += 1
            fieldsums[1][1] += temp

    def normalize_fieldsums(self, fieldsums):
	# remember the last average, but ignore the first one.
	if self.lastavgt:
	    self.lastavgt = fieldsums[1][1] / fieldsums[1][0]
	else:
	    self.lastavgt = fieldsums[1][1] / fieldsums[1][0]
	    # but if there wasn't one, don't output this one.
	    fieldsums[1][0] = 0

if __name__ == "__main__":
    if sys.argv[1] == 'voltage':
        data = Datavoltage()
    elif sys.argv[1] == 'pdepth':
        data = Datapdepth()
    else:
        data = Dataparser()
    writer = DatawriterCSV()
    data.dofiles(writer)

# EOF
