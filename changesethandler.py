'''
Content handler to implement an XML SAX parser

@author: Toby Murray
'''
import xml.sax.handler
import dateutil.parser

class ChangesetHandler(xml.sax.handler.ContentHandler):
    def __init__(self, connection):
        self.mapping = {}
        self.tags = dict()
        self.dbConnection = connection
        self.insertCount = 0
        
    def startElement(self, name, attrs):
        if name == "changeset":
            self.changeset = Changeset(attrs["id"])
            if "uid" in attrs:
                self.changeset.userId = attrs["uid"]
            self.changeset.createTime = dateutil.parser.parse(attrs["created_at"])
            if "min_lat" in attrs:
                self.changeset.minLat = attrs["min_lat"]
            if "max_lat" in attrs:
                self.changeset.maxLat = attrs["max_lat"]
            if "min_lon" in attrs:
                self.changeset.minLon = attrs["min_lon"]
            if "max_lon" in attrs:
                self.changeset.maxLon = attrs["max_lon"]
            if "closed_at" in attrs:
                self.changeset.closeTime = dateutil.parser.parse(attrs["closed_at"])
            if "num_changes" in attrs:
                self.changeset.numChanges = attrs["num_changes"]
            if "user" in attrs:
                self.changeset.userName = attrs["user"]
        elif name == "tag":
            self.tags[attrs["k"]] = attrs["v"]
            
    def endElement(self, name):
        if name == "changeset":
            '''insert into database'''
            cursor = self.dbConnection.cursor()
            cursor.execute('''INSERT into osm_changeset 
            (id, user_id, created_at, min_lat, max_lat, min_lon, max_lon, closed_at, num_changes, user_name, tags) 
            values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)''', (self.changeset.id, self.changeset.userId, self.changeset.createTime, self.changeset.minLat, 
                                                  self.changeset.maxLat, self.changeset.minLon, self.changeset.maxLon, self.changeset.closeTime, 
                                                  self.changeset.numChanges, self.changeset.userName,self.tags))
            self.insertCount += 1
            if self.insertCount % 10000 == 0:
                print "inserted {:,}".format(self.insertCount)
            cursor.close()
            self.tags.clear()
            
    def endDocument(self):
        self.dbConnection.commit()
        print 'finished inserting {:,} records'.format(self.insertCount)
            
class Changeset():
    def __init__(self, changeId):
        self.id = changeId
        self.userId = None
        self.createTime = None
        self.minLat = None
        self.maxLat = None
        self.minLon = None
        self.maxLon = None
        self.closeTime = None
        self.numChanges = None
        self.userName = None