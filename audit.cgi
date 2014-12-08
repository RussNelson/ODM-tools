#!/usr/bin/python

import os, sys, re, time, math, pwd, glob, datetime

modelbody = """Content-Type: text/html

<html>
<head><title>What's Up at RA-TES</title></head>
<body>
What's up?  Our sensors, that's what.  On %s Eastern, here are the sensors that
are, if green, phoned home, if blue, are not deployed, and if red, are deployed and down.
<table>
<tr><th>Site</th><th>Port</th><th>Last uploaded</th><th>Online for</th><th>Deployed</th><th>Sensors</th><th>first-reporting-date</th><th>last-reporting-date</th></tr>
|<tr><td><font color="%s">%s</font></td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>
|</table>
</body>
</html>
"""

hosts = [("2627", "B1", "/home/b1", "Dennings Point"),
         ("2628", "B2", "/home/b2", "Old Main"),
         ("2630", "B4", "/home/b4", ""),
         ("2631", "DApple", "/home/dapple", "Dutch Apple"),
         ("2629", "met1", "/home/met1", ""),
         ("2632", "ctd2", "/home/ctd2", "Jimmy field backup"),
         ("2633", "s3", "/home/s3", "Old Main test JR2 (cell stick borrowed)"),
         ("2634", "s4", "/home/s4", "Russ's Lawn"),
         ("2635", "s5", "/home/s5", ""),
         ("2636", "s6", "/home/s6", "Old Main test JR1"),
         ("2637", "s7", "/home/s7", "Court's Lawn"),
         ("2638", "s8", "/home/s8", "Old Main test JR3"),
         ("2639", "s9", "/home/s9", "Russ Desk test"),
         ("2640", "s10", "/home/s10", "Freemans Bridge"),
         ("2641", "s11", "/home/s11", "Russ Desk test"),
         ("2642", "s12", "/home/s12", ""),
         ("2643", "s13", "/home/s13", ""),
         ("2644", "s14", "/home/s14", ""),
         ("2645", "s15", "/home/s15", ""),
         ("2646", "s16", "/home/s16", ""),
         ("2647", "s17", "/home/s17", "remotetd RS422 test"),
         ("2648", "s18", "/home/s18", ""),
         ("2649", "s19", "/home/s19", ""),
         ("2650", "s20", "/home/s20", ""),
         ("2651", "s21", "/home/s21", ""),
         ("2652", "s22", "/home/s22", ""),
         ("2653", "s23", "/home/s23", ""),
         ("2654", "s24", "/home/s24", "Russ Desk test"),
         ("2655", "s25", "/home/s25", "Newburgh"),
         ("2701", "r1", "/home/r1", "Corinth"),
         ("2702", "r2", "/home/r2", ""),
         ("2703", "r3", "/home/r3", ""),
         ("2704", "r4", "/home/r4", "former St. Regis"),
         ("2705", "r5", "/home/r5", ""),
         ("2706", "r6", "/home/r6", ""),
         ("2707", "r7", "/home/r7", ""),
         ("2708", "r8", "/home/r8", "former Hinckley"),
         ("2709", "r9", "/home/r9", "North Creek"),
         ("2710", "r10", "/home/r10", "St. Regis"),
         ("2711", "r11", "/home/r11", "Grasse"),
         ("2712", "r12", "/home/r12", "LED display"),
         ("2713", "r13", "/home/r13", "testing"),
         ("2716", "r16", "/home/r16", "Hinckley"),
         ("2717", "r17", "/home/r17", "Piermont"),
         ("2719", "r19", "/home/r19", "Lock8"),
         ("2721", "r21", "/home/r21", "Schodack"),
         ("2722", "r22", "/home/r22", "Indian Lake"),
        ]

def dirwalk(dir):
    "walk a directory tree, using a generator"
    for f in os.listdir(dir):
        if f == '.ssh': continue
        if f == 'burnin': continue
        fullpath = os.path.join(dir,f)
        if os.path.isdir(fullpath) and not os.path.islink(fullpath):
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
    sys.stdout.write(model[0] % time.ctime())

    # get the boot time
    for line in open("/proc/stat"):
        fields = line.split()
        if fields[0] == 'btime':
            btime = int(fields[1])

    # set up a mapping from uid to username
    uid2user = {}
    for port, username, homedir, deployed in hosts:
        uid = pwd.getpwnam(username.lower()).pw_uid
        uid2user[uid] = username.lower()

    # search through the processes
    # make a dict which turns username into online time
    online = {}
    for file in glob.glob('/proc/[0-9]*/status'):
        try:
            for line in open(file):
                fields = line.split(':')
                if fields[0] == 'Uid':
                    uidfields = fields[1].split()
                    if int(uidfields[0]) in uid2user.keys():
                        fn = file.replace('status', 'stat')
                        fnstat = open(fn).readline().split()
                        online[uid2user[int(uidfields[0])]] = time.time() - (btime + int(fnstat[21]) / 100.0)
        except:
            # the process has already exited.
            pass

    inf = os.popen("netstat -tna", "r")
    ports = []
    for line in inf:
        fields = line.split()
        # tcp        0      0 0.0.0.0:2627            0.0.0.0:*               LISTEN
        match = re.match(r'0\.0\.0\.0:(.*)', fields[3])
        if match:
            ports.append(match.group(1))
    for port, host, homedir, deployed in hosts:
        if port in ports:
            color = "green"
        elif deployed:
            color = "red"
        else:
            color = "blue"
        lateness = time.time() - os.path.getmtime(os.path.join(homedir, ".temp"))
        if host.lower() in online.keys():
            on = dhms(online[host.lower()])
        else:
            on = "offline"
        sys.stdout.write(model[1] % (color, host, port, dhms(lateness), on, deployed, "", "", ""))
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
            if lastdate != '0':
                dt = datetime.datetime(*time.strptime(lastdate, "%Y%m%d%H")[:4])
                print k[-1], dt, (datetime.datetime.now() - dt).seconds
                if (datetime.datetime.now() - dt).seconds < 2*60*60 + 1:
                    lastdate = '<font color="green">%s</font>' % lastdate
            sensors.append (model[1] % ("black", "", "", "", "", "", lastfound.group(1), firstfound.group(2), lastdate) )
        sensors.sort()
        for sensor in sensors:
            sys.stdout.write(sensor)
    sys.stdout.write(model[2])

main()
