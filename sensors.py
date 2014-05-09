#!/usr/bin/python
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
0,Dutch Apple,Albany,s14:windair-02,DEPTH-1,voltage-s14,ppal-01
1,Beacon,Dutchess,s7:windair-04,ppal1-02
2,Stillwater,Saratoga,s8:windair-10,voltage-s8,pdepth2-2,pbar2-3,ppal1-03
3,Freemans Bridge,Schenectady,s10:windair-05,pdepth-01,ppal1-01
4,Old Main test,Saint Lawrence,r17:pbar2-16,pH-10,windair-35,COND-10,DO2-10,voltage-r17,pdepth2-19
5,West Point,Orange,s17:pdepth2-4,pbar-02
6,Newburgh,Orange,s25:pdepth1-14,pbar-04
7,North Creek,Warren,r9:windair-11,voltage-r9,pdepth1-02,pbar1-1,ppal1-06
8,Russ Lawn,Saint Lawrence,s4:voltage-s4
9,Old Main back lawn,Saint Lawrence,s6:windair-01,ppal2-0,pbar2-x
12,Corinth,Saratoga,r1:windair-40,pbar2-08,pdepth1-19,ppal1-08
14,St. Regis,Saint Lawrence,r4:COND-3,pdepth1-20,pbar2-10,DO-3,ppal1-09,OBS-10,windair-48,pH-3,voltage-r10
15,Hinckley,Herkimer,r16:pbar2-4,pH-1,windair-41,OBS-9,COND-1,DO-1,ppal1-07,pdepth2-8,fl3-414
16,Grasse,Saint Lawrence,r11:pH-0,COND-1,DO-1,ppal1-07,pdepth1-18,ppal1-14,windair-01,pbar2-7,voltage-r11
17,Racquette,Saint Lawrence,r7:COND-1,ppal1-15,DO-1,pH-1,pbar2-5,windair-18,pdepth2-3,voltage-r7
18,Lock 8,Schenectady,r19:pbar2-17,ppal1-17,pdepth2-11,windair-25,pdepth2-7,voltage-r19
19,Schodack,Rensselaer,r21:pbar2-13,pdepth2-17,pdepth2-9,ppal1-16,windair-38,voltage-r21
20,Indian Lake,Hamilton,r22:pbar2-18,pdepth2-19,pdepth2-15,voltage-r22
21,Piermont,Rockland,r17:pbar2-16,pdepth2-20,pdepth2-16,windair-35,voltage-r17
22,RATES Barn,St. Lawrence,r37:voltage-r37
"""
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
    Water level,bucketdepthdeep,54,Precipitation,Rain gauge detailed depth  
pdepth:
    Gage height,highaccuracypressure,47,Surface Water,High accuracy
    Temperature,temperature,96,Surface Water,
pdepth1:
    ,,,,
    Gage height,highaccuracypressure,47,Surface Water,High accuracy
    Gage height,largerangepressure,47,Surface Water,Large range
    Temperature,temperature,96,Surface Water,
pdepth2:
    ,,,,
    Gage height,highaccuracypressure,47,Surface Water,High accuracy
    Gage height,largerangepressure,47,Surface Water,Large range
    Temperature,temperature,96,Surface Water,
    Temperature,boardtemperature,96,Other,PCB temperature
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
    Chlorophyll a,chlorophyll,206,Surface Water,Optical sensor
    ,,,,
    ,,,,
    ,,,,
    Colored Dissolved Organic Matter,cdom,206,Surface Water,Optical sensor
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
    Current,chargecurrent,378,Not Relevant,Solar Charge
    Current,loadcurrent,378,Not Relevant,Load
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
          221:"NTU", 257:"#", 121:'mm/hr', 378:'A', 305:"mm/day" }

