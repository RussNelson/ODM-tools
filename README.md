ODM-tools
=========

HIS/ODM MySQL tools written in Python

  * database.py: read data from RTHS sensor data files, average, apply calibration, and insert into the database.
  * viz.cgi: display graphs of data using sites and seriescatalogs.
  * viz.html: templates for viz.cgi
  * viz.css: CSS to go with viz.html
  * update-series-catalog.py: update the SeriesCatalog table.
  * update-odm-cv.py: update the Controlled Vocabulary tables.
  * audit.cgi: a table of all the sites and sensors that have ever recorded data.
  * audit.py
  * locations.csv: sitename, lat, lon, suppress/redirect, elevation (suppress if X, a URL if redirect)
  * sensors.py: data structures describing our sensors
  * status.cgi: a live status report of the network
  * whatsup.cgi: status report of the network

