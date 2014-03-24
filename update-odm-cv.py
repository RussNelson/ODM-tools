#!/usr/bin/python
""" http://his.cuahsi.org/mastercvreg/cv11.aspx """

import suds, xml.sax

def tname(t):
    return t.lower()

class ABContentHandler(xml.sax.ContentHandler):
  def __init__(self, tn):
    self.tn = tn;
    xml.sax.ContentHandler.__init__(self)
 
  def startElement(self, name, attrs):
    #print("startElement '" + name + "'")
    #if name == "address":
    #  print(("\tattribute type='" + attrs.getValue("type") + "'").encode('utf-8'))
    if name == 'Record':
        self.tablerow = {}
    else:
        self.name  = name
 
  def endElement(self, name):
    #print(("endElement '" + name + "'").encode('utf-8'))
    if name == 'Record':
        columns = self.tablerow.keys()
        insert = 'INSERT ignore INTO %s (%s) VALUES (%s);' % (
            tname(self.tn),
            ", ".join(columns),
            ", ".join(["'%s'" % self.tablerow[k] for k in columns])
        )
        print insert.encode('utf-8')

    else:
        pass
 
  def characters(self, content):
    #print(("characters '" + content + "'").encode('utf-8'))
    self.tablerow[self.name] = content
 
c = suds.client.Client('http://his.cuahsi.org/odmcv_1_1/odmcv_1_1.asmx?WSDL')


#for method in c.wsdl.services[0].ports[0].methods.values():
#    print "service(c.service.Get%s, '%s')" % ( method.name, method.name[3:])

def service(call, name):
    data = call().encode('utf-8')
    xml.sax.parseString(data, ABContentHandler(name))


# Pure hackery. There MUST be a way to look at the WSDL and call the functions below.
# I hate SOAP, and it hates me.
service(c.service.GetValueTypeCV, 'ValueTypeCV')
service(c.service.GetVariableNameCV, 'VariableNameCV')
service(c.service.GetDataTypeCV, 'DataTypeCV')
service(c.service.GetUnits, 'Units')
service(c.service.GetSampleMediumCV, 'SampleMediumCV')
service(c.service.GetVerticalDatumCV, 'VerticalDatumCV')
service(c.service.GetCensorCodeCV, 'CensorCodeCV')
service(c.service.GetSampleTypeCV, 'SampleTypeCV')
service(c.service.GetTopicCategoryCV, 'TopicCategoryCV')
service(c.service.GetSpeciationCV, 'SpeciationCV')
service(c.service.GetGeneralCategoryCV, 'GeneralCategoryCV')
service(c.service.GetSpatialReferences, 'SpatialReferences')
service(c.service.GetSiteTypeCV, 'SiteTypeCV')

