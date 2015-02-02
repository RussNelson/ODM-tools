# vim: set ai sw=4 sta et fo=croql

# The structure of this data is: one site per line.
# The line is separated into two halves by a colon.
# The first half is a comma-separated list containing:
#     the ***permanent*** site number,
#     name of the site,
#     county of the site,
#     the hostname of the computer hosting the sensors.
# The second half is a comma-separated list of sensor model and serials.
rths_sites = """
35,Dutch Apple,Albany,r63:windair-32,DEPTH-1,ppal-01
1,Beacon,Dutchess,r64:windair-04,ppal1-02
2,Stillwater,Saratoga,r55:pbar4-20,ppal1-29,pdepth2-2,windair-40
3,Freemans Bridge,Schenectady,s10:windair-05,pdepth-01,ppal1-01
4,Old Main test,Saint Lawrence,r17:
5,West Point,Orange,s17:
6,Newburgh,Orange,r39:pdepth1-14,pbar-04
7,North Creek,Warren,r9:windair-11,voltage-r9,pdepth1-02,pbar1-1,ppal1-06
9,Old Main back lawn,Saint Lawrence,r5:windair-01,ppal2-0
12,Corinth,Saratoga,r42:pdepth1-19,windair-42,ppal1-08,pbar4-4
14,St. Regis,Saint Lawrence,r4:pdepth1-20,pbar2-10,ppal1-09,windair-48,voltage-r10
15,Hinckley,Herkimer,r16:pbar2-4,pH-1,windair-41,OBS-9,COND-1,DO-1,ppal1-07,pdepth2-8,fl3-414
16,Grasse,Saint Lawrence,r11:pH-0,COND-1,DO-1,ppal1-07,pdepth1-18,ppal1-14,windair-01,pbar2-7,voltage-r11
17,Racquette,Saint Lawrence,r7:COND-1,ppal1-15,DO-1,pH-1,pbar2-5,windair-18,pdepth2-3,voltage-r7
18,Lock 8,Schenectady,r19:pbar2-17,ppal1-17,pdepth2-11,windair-25,pdepth2-7,voltage-r19
19,Schodack,Rensselaer,r21:pbar2-13,pdepth2-17,pdepth2-9,ppal1-16,windair-38,voltage-r21
20,Indian Lake,Hamilton,r22:pbar2-18,pdepth2-19,pdepth2-15,voltage-r22
21,Piermont,Rockland,r17:pbar2-16,pdepth2-20,pdepth2-16,windair-35,voltage-r17
22,RATES Barn,St. Lawrence,r37:voltage-r37
23,Frankfort,Herkimer,r32:pbar2-25,pdepth2-39,pdepth2-45,ppal1-40,windair-34
24,Newcomb,Essex,r18:pbar2-23
25,Tahawus,Essex,r20:pbar2-20,windair-22
26,Esperance,Schoharie,r30:windair-15,pbar2-22,pdepth2-32,pdepth2-74,ppal1-39
27,Lexington,Greene,r26:windair-30,pbar4-8,pdepth2-50,pdepth2-76,ppal1-28
28,Germantown,Columbia,r41:
29,Cedar River,Hamilton,r23:
30,Whites,Dutchess,r20:
31,Powley,Fulton,r34:
32,Piseco,Hamilton,r27:
33,Durham,Greene,r31:
34,Wallkill,Ulster,r50:
36,Arietta,Hamilton,r29:
37,Wells,Hamilton,r18:
38,Croton,Westchester,r49:
39,Moreau,Saratoga,r36:
40,Herkimer,Herkimer,r48:
41,Nelliston,Montgomery,r52:
42,Athens,Greene,r51:
43,Glasco,Ulster,r33:
44,Ulster,Ulster,r47:
45,Luzerne,Warren,r28:
46,Rondout,Ulster,r54:
47,Poland,Herkimer,r43:
48,Haverstraw,Westchester,r57:
49,Schuylerville,Saratoga,r40:
50,Normanskill,Albany,r29:
"""
#8,Russ Lawn,Saint Lawrence,s4:voltage-s4
#13,St. Regis,Saint Lawrence,r1:windair-40,pbar2-8,pdepth1-19
#11,St. Regis,Saint Lawrence,r4:COND-0,pdepth1-20,pbar2-01,DO-0,ppal1-09,OBS-8,windair-23,pH-0

other_sites = """
4,Old Main back lawn,Saint Lawrence,s24:windair-09
Old Main,s23:MBIRDS-5
10,Evans and Whites,Saint Lawrence,s10:MBIRDS-2
"""

rths_sites = [(a[0].split(","), a[1].split(",")) for a in [a.split(":") for a in rths_sites.split("\n")[1:-1]]]



# The structure of this data is: one sensor per line
#   Except for first and last blank lines,
#   and indented lines are folded and have a semicolon inserted
# The sensors are ordered in the same order as the columns in the data file.
# The line is separated into two halves by a colon.
# The first half is the sensor model name.
# The second half is split into measurements by a semicolon.
# Each measurement has these parts: the name of the parameter being measured,
#   the codename of the parameter, 
#   the units of the parameter,
#   the sample medium of the parameter (Air or Surface Water),
#   the database name of the parameter.
#   insert into variables (VariableCode, VariableName, VariableUnitsID, SampleMedium, ValueType, IsRegular, TimeSupport, TimeUnitsID, DataType, GeneralCategory, NoDataValue) values ('precipdaily', 'Precipitation', 305, 'Precipitation', 'Field Observation', 1, 0, 100, 'Continuous', 'Instrumentation', -999);
#
rths_sensors = """
windair:
    Wind speed,windspeed,119,Air,
    Wind direction,winddir,2,Air,
    Temperature,airtemp,96,Air,
    Relative Humidity,humidity,1,Air,
ppal:
    Precipitation,precip,121,Precipitation,
    Water level,bucketdepth,54,Precipitation,Rain gauge extensive depth
    Temperature,buckettemp,96,Precipitation,
    Water level,bucketdepthdeep,54,Precipitation,Rain gauge detailed depth  
ppal1:
    Precipitation,precip,121,Precipitation,
    Water level,bucketdepth,54,Precipitation,Rain gauge extensive depth
    Temperature,buckettemp,96,Precipitation,
    Water level,bucketdepthdeep,54,Precipitation,Rain gauge detailed depth  
ppal2:
    Precipitation,precip,121,Precipitation,
    Water level,bucketdepth,54,Precipitation,Rain gauge extensive depth
    Temperature,buckettemp,96,Precipitation,
ppal3:
    Temperature,buckettemp,96,Precipitation,
    Temperature,boardtemperature,96,Other,ppal3 PCB temperature
    Water level,bucketdepthdeep,54,Precipitation,Rain gauge detailed depth  
    Water level,bucketdepth,54,Precipitation,Rain gauge extensive depth
ppaltest:
    ,,,,
    Temperature,buckettemp,96,Precipitation,
    Water level,bucketdepth,54,Precipitation,Rain gauge extensive depth
pdepth:
    Gage height,highaccuracypressure,47,Surface Water,High accuracy
    Temperature,temperature,96,Surface Water,
pdepth1:
    ,,,,
    Gage height,highaccuracypressure,47,Surface Water,High accuracy
    Gage height,largerangepressure,47,Surface Water,Large range
    Temperature,temperature,96,Surface Water,
    Water level,waterlevel,52,Surface Water,
pdepth2:
    ,,,,
    Gage height,highaccuracypressure,47,Surface Water,High accuracy
    Gage height,largerangepressure,47,Surface Water,Large range
    Temperature,temperature,96,Surface Water,Temperature
    Temperature,boardtemperature,96,Other,PCB temperature
    Water level,waterlevel,52,Surface Water,
pbar:
    Temperature,cabinettemperature,96,Other,Cabinet temperature
    Barometric Pressure,baro,90,Air,
pbar1:
    Temperature,cabinettemperature,96,Other,Cabinet temperature
    Barometric Pressure,baro,90,Air,
    ,,,,
pbar2:
    Temperature,cabinettemperature,96,Other,
    Barometric Pressure,baro,90,Air,
pbar3:
    Temperature,cabinettemperature,96,Other,
    Barometric Pressure,baro,90,Air,
pbar4:
    Temperature,cabinettemperature,96,Other,
    Barometric Pressure,baro,90,Air,
DEPTH:
    Gage height,depth,47,Surface Water,
DEPTH2:
    Gage height,depth2highaccuracy,47,Surface Water,High accuracy
    Gage height,depth2largerange,47,Surface Water,Large range
MBIRDS:
    Temperature,mbwatertemp,96,Surface Water,Infrared sensing
    Temperature,mbairtemp,96,Air,Unshielded sensor
    Gage height,mbdepth,47,Surface Water,Ultrasonic distance ranging
COND:
    Electrical conductivity,lowcond,192,Surface Water,Low range sensor
    Electrical conductivity,mediumcond,192,Surface Water,Medium range sensor
    Electrical conductivity,highcond,192,Surface Water,High range sensor
fl3:
    ,,,,
    Chlorophyll a,chlorophyllm,199,Surface Water,Optical sensor
    ,,,,
    ,,,,
    ,,,,
    Colored Dissolved Organic Matter,cdomm,199,Surface Water,Optical sensor
    ,,,,
OBS:
    ,,,,
    ,,,,
    ,,,,
    Turbidity,optical-red,221,Surface Water,Red
    Turbidity,reference,257,Surface Water,Reference sensor
    ,,,,
    Turbidity,optical-green,221,Surface Water,Green
    Turbidity,optical-blue,221,Surface Water,Blue
pH:
    pH,ph,309,Surface Water,ion exchange
DO:
    Oxygen%2C dissolved,do,199,Surface Water,galvanic response
DO2:
    Oxygen%2C dissolved,dopercent,1,Surface Water,galvanic response
    Oxygen%2C dissolved,do,199,Surface Water,galvanic response
DO3:
    Oxygen%2C dissolved,do,199,Surface Water,galvanic response
voltage:
    Battery Voltage,batteryvoltage,168,Not Relevant,
    Electric Current,chargecurrent,378,Not Relevant,Solar Charge
    Electric Current,loadcurrent,378,Not Relevant,Load
optode:
    Oxygen%2C dissolved,do,199,Surface Water,optode
    Oxygen%2C dissolved,dopercent,1,Surface Water,optode
    Temperature,temperature,96,Surface Water,optode
    ,,,,
    ,,,,
    ,,,,
    ,,,,
    ,,,,
    ,,,,
    ,,,,
CDOMMC:
    ,,,,
    Colored Dissolved Organic Matter,cdom,205,Surface Water,Optical sensor
    ,,,,
    ,,,,
CHLAMB:
    ,,,,
    Chlorophyll a,chlorophyll,206,Surface Water,Optical sensor
    ,,,,
    ,,,,
OBSBMA:
    ,,,,
    Turbidity,optical-blue,221,Surface Water,Blue
    ,,,,
    ,,,,
OBSBMD:
    ,,,,
    Turbidity,optical-blue,221,Surface Water,Blue
    ,,,,
    ,,,,
OBSGMA:
    ,,,,
    Turbidity,optical-green,221,Surface Water,Green
    ,,,,
    ,,,,
OBSGMD:
    ,,,,
    Turbidity,optical-green,221,Surface Water,Green
    ,,,,
    ,,,,
OBSRMA:
    ,,,,
    Turbidity,optical-red,221,Surface Water,Red
    ,,,,
    ,,,,
OBSRMD:
    ,,,,
    Turbidity,optical-red,221,Surface Water,Red
    ,,,,
    ,,,,
"""

rths_sensors = rths_sensors .replace(":\n    ",":").replace("\n    ",";")

rths_sensors = dict([(a[0], [[d.replace("%2C",",") for d in b.split(",")] for b in a[1].split(";")]) for a in [c.split(":") for c in rths_sensors.split("\n")[1:-1]]])

# The structure of this data is: one sensor per line.
# The line is separated into two halves by a colon.
# The first half is the sensor model name.
# The second half is a description of the sensor.
rths_descriptions = """windair:A shielded air temperature and relative humidity sensor mounted on the same pole as an anememometer and wind vane.
ppal:A water collection tube with depth gauge.
ppal1:A water collection tube with depth gauge.
ppal2:A water collection tube with embedded depth gauge.
pdepth:An encapsulated differential pressure sensor measuring water column height.
pdepth1:An encapsulated differential pressure sensor measuring water column height and water temperature.
pdepth2:An encapsulated differential pressure sensor measuring water column height and water temperature.
pbar:A barometer and temperature gauge (which is inside our electronics enclosure).
pbar1:A barometer and temperature gauge (which is inside our electronics enclosure).
pbar2:A barometer and temperature gauge (which is inside our electronics enclosure).
pbar3:A barometer and temperature gauge (which is inside our electronics enclosure).
pbar4:A barometer and temperature gauge (which is inside our electronics enclosure).
DEPTH:An unencapsulated differential pressure sensor measuring water column height.
DEPTH2:An unencapsulated differential pressure sensor measuring water column height at two resolutions and ranges.
MBIRDS:An ultrasonic distance measure to the water surface below, an infrared temperature sensor, and an unshielded air temperature sensor.
COND:A direct measurement of the conductivity sampled at three ranges.
OBS:Optical Back Scatter measurement of the reflectance of the water at three wavelengths: visible red, green, and blue.
pH:An ion exchange pH probe.
DO:Dissolved Oxygen
voltage:Battery voltage"""
rths_descriptions = dict([ [a[0], a[1]] for a in [c.split(":") for c in rths_descriptions.split("\n")]])

# select distinct(VariableUnitsID), UnitsAbbreviation from seriescatalog, units where seriescatalog.VariableUnitsID = units.UnitsID;
units = { 54:"mm", 47:"cm", 168:"V", 199:"mg/l", 309:"pH", 100:"s", 52:"m", 1:"%",2:"deg",90:"mbar",96:"degC",119:"m/s", 192:"uS/cm",
          221:"NTU", 257:"#", 121:'mm/hr', 378:'A', 305:"mm/day", 206:"ppb", 205:"ppm" }

