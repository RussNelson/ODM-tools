#!/usr/bin/python

import os, sys, re, time, math, pwd, glob, datetime

modelbody = """Content-Type: text/html

<html>
<head><title>REON sensor list</title></head>
<body>
The list that follows contains a list of all the sensors we have ever received data from, not counting burnin data.<table>
<tr><th>Site</th><th>Host</th><th>Sensors</th><th>first-reporting-date</th><th>last-reporting-date</th></tr>
|<tr><td><font color="%s">%s</font></td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>
|</table>
</body>
</html>
"""

def dirwalk(dir):
    "walk a directory tree, using a generator"
    for f in os.listdir(dir):
        fullpath = os.path.join(dir,f)
        if os.path.isdir(fullpath) and not os.path.islink(fullpath):
            if not f.startswith("done"): continue
            for x in dirwalk(fullpath):  # recurse into subdir
                yield x
        else:
            yield fullpath

def dhms(interval):
    interval, seconds = divmod(interval, 60)
    interval, minutes = divmod(interval, 60)
    days, hours = divmod(interval, 24)
    interval = ""
    if days: interval += "%dd" % days
    if hours: interval += "%dh" % hours
    if minutes and len(interval) < 4: interval += "%dm" % minutes
    if seconds and len(interval) < 4: interval += "%ds" % seconds
    return interval

def main():
    model = modelbody.split('|')
    sys.stdout.write(model[0])

    sites = []
    for homedir in glob.glob("/user/*"):
        fn = homedir + "/.sitecode"
        if not os.path.exists(fn): continue
        sitecode = open(homedir + "/.sitecode").read().rstrip()
        if not sitecode: continue
        sites.append( (sitecode, homedir) )

    sites.sort()

    for sitecode, homedir in sites:
        host = homedir.split("/")[-1]
        sys.stdout.write(model[1] % ("black", sitecode, host, "", "", ""))
	# we construct a data structure which is a hash of arrays of filenames.
        logfiles = {}
        for file in dirwalk(homedir):
            found = re.search(r'log-(.*?-.*?)-', file)
	    if not found: continue
	    devname = found.group(1)
            if not devname in logfiles:
                logfiles[devname] = []
            logfiles[devname].append(file)
	# after we sort the arrays, we have the most recent filename.
        sensors = []
	for k in logfiles.values():
            k.sort()
            firstfound = re.search(r'log-(.*?-.*?)-(\d+)', k[0])
            lastfound = re.search(r'log-(.*?-.*?)-(\d+)', k[-1])
            lastdate = lastfound.group(2)
            if False and lastdate != '0':
                dt = datetime.datetime(*time.strptime(lastdate, "%Y%m%d%H")[:4])
                #print k[-1], dt, (datetime.datetime.now() - dt).seconds
                if (datetime.datetime.now() - dt).seconds < 2*60*60 + 1:
                    lastdate = '<font color="green">%s</font>' % lastdate
            sensors.append (model[1] % ("black", "", "", lastfound.group(1), firstfound.group(2), lastdate) )
        sensors.sort()
        for sensor in sensors:
            sys.stdout.write(sensor)
    sys.stdout.write(model[2])

main()
