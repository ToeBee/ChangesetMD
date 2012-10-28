'''
Content handler to implement an XML SAX parser

@author: Toby Murray
'''
import xml.sax.handler
import dateutil.parser

class ChangesetHandler(xml.sax.handler.ContentHandler):
    def __init__(self, connection, newestChangeset):
        self.mapping = {}
        self.tags = dict()
        self.dbConnection = connection
        self.insertCount = 0
        self.skipCount = 0
        self.startFromChangeset = newestChangeset
        
    def startElement(self, name, attrs):
        if name == "changeset":
            self.changeset = Changeset()
            self.changeset.id = attrs.get("id")
            self.changeset.userId = attrs.get('uid', None)
            self.changeset.createTime = dateutil.parser.parse(attrs.get('created_at'))
            self.changeset.minLat = attrs.get('min_lat', None)
            self.changeset.maxLat = attrs.get('max_lat', None)
            self.changeset.minLon = attrs.get('min_lon', None)
            self.changeset.maxLon = attrs.get('max_lon', None)
            if 'closed_at' in attrs:
                self.changeset.closeTime = dateutil.parser.parse(attrs.get('closed_at'))
            else:
                self.changeset.closeTime = None
            self.changeset.numChanges = attrs.get('num_changes', None)
            self.changeset.userName = attrs.get('user', None)
            self.changeset.open = attrs.get('open', None)
        elif name == "tag":
            self.tags[attrs["k"]] = attrs["v"]
            
    def endElement(self, name):
        if name == "changeset":
            if self.startFromChangeset != -1 and long(self.changeset.id) <= self.startFromChangeset:
                self.skipCount += 1
                if self.skipCount % 10000 == 0:
                    print "skipped {:,}".format(self.skipCount)
                    
            else:
                '''insert into database'''
                cursor = self.dbConnection.cursor()
                cursor.execute('''INSERT into osm_changeset 
                (id, user_id, created_at, min_lat, max_lat, min_lon, max_lon, closed_at, open, num_changes, user_name, tags) 
                values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)''', (self.changeset.id, self.changeset.userId, self.changeset.createTime, self.changeset.minLat, 
                                                  self.changeset.maxLat, self.changeset.minLon, self.changeset.maxLon, self.changeset.closeTime, self.changeset.open, 
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
    pass
