#!/usr/bin/python

import math

from stat import *
import os, sys, re, time, math, pwd, glob, pickle, stat, webcolors

from sensors import rths_sites

orange = (255, 100, 0)
orange = webcolors.name_to_rgb("orange")
red = webcolors.name_to_rgb("red")
palered = (255, 100, 100)
palered = webcolors.name_to_rgb("pink")
aqua = webcolors.name_to_rgb("cyan")
yellow = webcolors.name_to_rgb("yellow")
green = webcolors.name_to_rgb("green")
black = webcolors.name_to_rgb("black")

downhosts = [("2627", "B1", 5),
         ("2628", "B2", 5),
         ("2630", "B4", 5),
]

hosts = [
         ('2753', 'r53', 60) ,
         ('2758', 'r58', 60) ,
         ('2751', 'r51', 60) ,
         ('2729', 'r29', 60) ,
         ('2734', 'r34', 60) ,
         ('2727', 'r27', 60) ,
         ('2765', 'r65', 60) ,
         ('2766', 'r66', 60) ,
         ('2740', 'r40', 60) ,
         ('2759', 'r59', 60) ,
         ('2718', 'r18', 60) ,
         ('2760', 'r60', 60) ,
         ('2750', 'r50', 60) ,
         ('2736', 'r36', 60) ,
         ('2733', 'r33', 60) ,
         ('2757', 'r57', 60) ,
         ('2764', 'r64', 60) ,
         ('2723', 'r23', 60) ,
         ('2742', 'r42', 60) ,
         ('2749', 'r49', 60) ,
         ('2731', 'r31', 60) ,
         ('2763', 'r63', 5) ,
         ('2730', 'r30', 60) ,
         ('2732', 'r32', 60) ,
         ('2640', 's10', 60) ,
         ('2741', 'r41', 60) ,
         ('2748', 'r48', 60) ,
         ('2711', 'r11', 60) ,
         ('2716', 'r16', 60) ,
         ('2722', 'r22', 60) ,
         ('2726', 'r26', 60) ,
         ('2719', 'r19', 60) ,
         ('2728', 'r28', 60) ,
         ('2739', 'r39', 5) ,
         ('2735', 'r35', 60) ,
         ('2709', 'r9', 60) ,
         ('2705', 'r5', 60) ,
         ('2717', 'r17', 60) ,
         ('2707', 'r7', 60) ,
         ('2738', 'r38', 60) ,
         ('2754', 'r54', 60) ,
         ('2721', 'r21', 60) ,
         ('2755', 'r55', 60) ,
         ('2710', 'r10', 60) ,
         ('2714', 'r14', 60) ,
         ('2703', 'r3', 60) ,
         ('2720', 'r20', 60) ,
         ('2747', 'r47', 60) ,
       ][::-1]

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
    # get the boot time
    for line in open("/proc/stat"):
        fields = line.split()
        if fields[0] == 'btime':
            btime = int(fields[1])

    # check to see 
    out_of_date = {}
    for name, sensors in rths_sites:
        (id, name, county, rpi_name) = name
        for sensor in sensors:
            if not sensor: continue
            try:
                ftime = os.stat("timestamp-%s" % (sensor))[ST_MTIME]
            except OSError, err:
                if err[0] == 2: continue
                raise
            # if older than two hours ago
            oldness = 3 * 60 * 60
            if ftime < time.time() - oldness:
                #out_of_date[sensor] = ftime
                out_of_date[name] = out_of_date.get(name, 0) + 1
                #print 'The',sensor,'at',name,'is out of date by %s seconds' % (time.time() - ftime)

    # set up a mapping from uid to username
    uid2user = {}
    homes = {}
    for port, username, upload in hosts:
        pw = pwd.getpwnam(username.lower())
        uid2user[pw.pw_uid] = username.lower()
        homes[username] = pw.pw_dir

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

    hostmap = pickle.Unpickler(open("hosts/map.pickle")).load()
    values = []
    for port, host, upload in hosts:
        homedir = homes[host]
        r = 0
        g = 0
        b = 0
        lateness = time.time() - os.path.getmtime(os.path.join(homedir, ".temp"))
	deployed = hostmap.get(host, "")
        latereduce = max(0, int((math.log(lateness) - 4) * 9))
        if port in ports:
            r, g, b = green
            if lateness > upload * 60:
                r, g, b = yellow
        elif "test" in deployed:
            r, g, b = aqua
        elif deployed:
            r, g, b = red
            if lateness < upload * 60:
                r, g, b = palered
                if os.path.exists("/home/%s/.satellite" % host):
                    r, g, b = orange
	else:
            r, g, b = black
        sitefn = "/home/%s/.sitecode" % host
        if os.path.exists(sitefn):
            site = open(sitefn).read().rstrip()
        else:
            site = "unknown"
        values.append([r, g, b, host, site])
    if len(sys.argv) > 1:
        values.sort(lambda a,b:cmp(a[4], b[4] ))
        print "Content-Type: application/json\n"
        print '{"data":['
        print ",".join(['["%s", "%s", "%02x%02x%02x"]' % (host, site, r, g, b) for r,g,b,host,site in values])
        print ']}'
    else:
        for r,g,b,host,site in values:
            print r / 2, g / 2, b / 2, host, site

main()

