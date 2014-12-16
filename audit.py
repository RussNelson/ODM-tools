#!/usr/bin/python

import os

inf = os.popen("locate .sitecode")

for line in inf:
    fn = line.rstrip().replace(".sitecode","")
    for root, dirs, files in os.walk(fn):
        dirs = [ d for d in dirs if d.startswith("done") ]
        filelist = {}
        for name in files:
            fields = name.split("-")
            if fields[0] not in ['.log', 'log']: continue
            k = fields[1] + "-" + fields[3]
            if k in filelist:
                print root, name
            filelist = k
