#!/usr/bin/python

import os, sys, re, time, math, pwd, glob, pickle

modelbody = """Content-Type: text/html

<html>
<head><title>What's Up at RA-TES</title></head>
<body>
What's up?  Our sensors, that's what.  On %s Eastern, here are the sensors that
are, if green, phoned home, if blue, are not deployed, and if red, are deployed and down.
<form action="savefile.cgi" method="post" >
<table>
<tr><th>Site</th><th>Port</th><th>Last uploaded</th><th>Online for</th><th>Deployed</th></tr>
|<tr><td><font color="%s">%s</font></td><td>%s</td><td><font color="%s">%s</font></td><td>%s</td><td><input name="%s" value="%s" /><br/></td></tr>
|</table>
<input class="button" type="submit" value="Save Locations" />
</form>
</body>
</html>
"""

hosts = [("2627", "B1"),
         ("2628", "B2"),
         ("2630", "B4"),
         ("2640", "s10"),
         ("2702", "r2"),
         ("2703", "r3"),
         ("2705", "r5"),
         ("2706", "r6"),
         ("2707", "r7"),
         ("2709", "r9"),
         ("2710", "r10"),
         ("2711", "r11"),
         ("2712", "r12"),
         ("2713", "r13"),
         ("2714", "r14"),
         ("2715", "r15"),
         ("2716", "r16"),
         ("2717", "r17"),
         ("2718", "r18"),
         ("2719", "r19"),
         ("2720", "r20"),
         ("2721", "r21"),
         ("2722", "r22"),
         ("2723", "r23"),
         ("2724", "r24"),
         ("2725", "r25"),
         ("2726", "r26"),
         ("2727", "r27"),
         ("2728", "r28"),
         ("2729", "r29"),
         ("2730", "r30"),
         ("2731", "r31"),
         ("2732", "r32"),
         ("2733", "r33"),
         ("2734", "r34"),
         ("2735", "r35"),
         ("2736", "r36"),
         ("2737", "r37"),
         ("2738", "r38"),
         ("2739", "r39"),
         ("2740", "r40"),
         ("2741", "r41"),
         ("2742", "r42"),
         ("2744", "r44"),
         ("2745", "r45"),
         ("2746", "r46"),
         ("2747", "r47"),
         ("2748", "r48"),
         ("2749", "r49"),
         ("2750", "r50"),
         ("2751", "r51"),
         ("2752", "r52"),
         ("2753", "r53"),
         ("2754", "r54"),
         ("2755", "r55"),
         ("2756", "r56"),
         ("2757", "r57"),
         ("2758", "r58"),
         ("2759", "r59"),
         ("2760", "r60"),
         ("2761", "r61"),
         ("2762", "r62"),
         ("2763", "r63"),
         ("2764", "r64"),
         ("2765", "r65"),
        ]

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
    homes = {}
    for port, username in hosts:
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
    for port, host in hosts:
        homedir = homes[host]
        lateness = time.time() - os.path.getmtime(os.path.join(homedir, ".temp"))
	latenesscolor = "black"
        if host.lower() in online.keys():
            on = dhms(online[host.lower()])
        else:
            on = "offline"
	deployed = hostmap.get(host, "")
        if port in ports:
            color = "green"
            if lateness < 70 * 60:
                latenesscolor = "green"
        elif deployed and deployed.find("sleeping") >= 0:
	    color = "aqua"
        elif deployed and deployed.find("test") >= 0:
	    color = "blue"
        elif deployed:
            color = "red"
	else:
	    color = "black"
        sys.stdout.write(model[1] % (color, host, port, latenesscolor, dhms(lateness), on, host, deployed))
    sys.stdout.write(model[2])

main()
