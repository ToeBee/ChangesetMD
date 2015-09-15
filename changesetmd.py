#!/usr/bin/python
'''
ChangesetMD is a simple XML parser to read the weekly changeset metadata dumps
from OpenStreetmap into a postgres database for querying.

@author: Toby Murray
'''

import os
import sys
import argparse
import psycopg2
import psycopg2.extras
import queries
import gzip
import urllib2
from lxml import etree
from datetime import datetime
from datetime import timedelta
from StringIO import StringIO

try:
    from bz2file import BZ2File
    bz2Support = True
except ImportError:
    bz2Support = False

BASE_REPL_URL = "http://planet.osm.org/replication/changesets/"

class ChangesetMD():
    def truncateTables(self, connection):
        print 'truncating tables'
        cursor = connection.cursor()
        cursor.execute("TRUNCATE TABLE osm_changeset_comment CASCADE;")
        cursor.execute("TRUNCATE TABLE osm_changeset CASCADE;")
        cursor.execute(queries.dropIndexes)
        connection.commit()

    def createTables(self, connection):
        print 'creating tables'
        cursor = connection.cursor()
        cursor.execute(queries.createChangesetTable)
        connection.commit()

    def insertNew(self, connection, id, userId, createdAt, minLat, maxLat, minLon, maxLon, closedAt, open, numChanges, userName, tags, comments):
        cursor = connection.cursor()
        cursor.execute('''INSERT into osm_changeset
                    (id, user_id, created_at, min_lat, max_lat, min_lon, max_lon, closed_at, open, num_changes, user_name, tags)
                    values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)''',
                    (id, userId, createdAt, minLat, maxLat, minLon, maxLon, closedAt, open, numChanges, userName, tags))
        for comment in comments:
            cursor.execute('''INSERT into osm_changeset_comment
                    (comment_changeset_id, comment_user_id, comment_user_name, comment_date, comment_text)
                    values (%s,%s,%s,%s,%s)''',
                    (id, comment['uid'], comment['user'], comment['date'], comment['text']))

    def deleteExisting(self, connection, id):
        cursor = connection.cursor()
        cursor.execute('''DELETE FROM osm_changeset_comment
                          WHERE comment_changeset_id = %s''', (id,))
        cursor.execute('''DELETE FROM osm_changeset
                          WHERE id = %s''', (id,))

    def parseFile(self, connection, changesetFile, doReplication):
        parsedCount = 0
        startTime = datetime.now()
        cursor = connection.cursor()
        cursor.execute('''SET synchronous_commit TO OFF''')
        context = etree.iterparse(changesetFile)
        action, root = context.next()
        for action, elem in context:
            if(elem.tag != 'changeset'):
                continue

            parsedCount += 1

            tags = {}
            for tag in elem.iterchildren(tag='tag'):
                tags[tag.attrib['k']] = tag.attrib['v']

            comments = []
            for discussion in elem.iterchildren(tag='discussion'):
                for commentElement in discussion.iterchildren(tag='comment'):
                    comment = dict()
                    comment['uid'] = commentElement.attrib.get('uid')
                    comment['user'] = commentElement.attrib.get('user')
                    comment['date'] = commentElement.attrib.get('date')
                    for text in commentElement.iterchildren(tag='text'):
                        comment['text'] = text.text
                    comments.append(comment)

            if(doReplication):
                print 'deleting potentially existing changeset: ', elem.attrib['id']
                self.deleteExisting(connection, elem.attrib['id'])

            self.insertNew(connection, elem.attrib['id'], elem.attrib.get('uid', None),
                           elem.attrib['created_at'], elem.attrib.get('min_lat', None),
                           elem.attrib.get('max_lat', None), elem.attrib.get('min_lon', None),
                           elem.attrib.get('max_lon', None),elem.attrib.get('closed_at', None),
                           elem.attrib.get('open', None), elem.attrib.get('num_changes', None),
                           elem.attrib.get('user', None), tags, comments)

            if((parsedCount % 10000) == 0):
                print "parsed %s" % ('{:,}'.format(parsedCount))
                print "cumulative rate: %s/sec" % '{:,.0f}'.format(parsedCount/timedelta.total_seconds(datetime.now() - startTime))
            
            #clear everything we don't need from memory to avoid leaking
            elem.clear()
            while elem.getprevious() is not None:
                del elem.getparent()[0]
        connection.commit()
        print "parsing complete"
        print "parsed {:,}".format(parsedCount)


    def fetchReplicationFile(self, connection, sequenceNumber):
        topdir = format(sequenceNumber / 1000000, '003')
        if(sequenceNumber >= 1000000):
            sequenceNumber = sequenceNumber - 1000000
        subdir = format(sequenceNumber / 1000, '003')
        fileNumber = format(sequenceNumber % 1000, '003')
        fileUrl = BASE_REPL_URL + topdir + '/' + subdir + '/' + fileNumber + '.osm.gz'
        print "opening replication file at " + fileUrl
        replicationFile = urllib2.urlopen(fileUrl)
        replicationData = StringIO(replicationFile.read())
        return gzip.GzipFile(fileobj=replicationData)

if __name__ == '__main__':
    beginTime = datetime.now()
    endTime = None
    timeCost = None

    argParser = argparse.ArgumentParser(description="Parse OSM Changeset metadata into a database")
    argParser.add_argument('-t', '--trunc', action='store_true', default=False, dest='truncateTables', help='Truncate existing tables (also drops indexes)')
    argParser.add_argument('-c', '--create', action='store_true', default=False, dest='createTables', help='Create tables')
    argParser.add_argument('-H', '--host', action='store', dest='dbHost', help='Database hostname')
    argParser.add_argument('-P', '--port', action='store', dest='dbPort', default=None, help='Database port')
    argParser.add_argument('-u', '--user', action='store', dest='dbUser', default=None, help='Database username')
    argParser.add_argument('-p', '--password', action='store', dest='dbPass', default=None, help='Database password')
    argParser.add_argument('-d', '--database', action='store', dest='dbName', help='Target database', required=True)
    argParser.add_argument('-f', '--file', action='store', dest='fileName', help='OSM changeset file to parse')
    argParser.add_argument('-r', '--replicate', action='store_true', dest='doReplication', default=False, help='Apply a replication file to an existing database')
    
    args = argParser.parse_args()

    conn = psycopg2.connect(database=args.dbName, user=args.dbUser, password=args.dbPass, host=args.dbHost, port=args.dbPort)


    md = ChangesetMD()
    if args.truncateTables:
        md.truncateTables(conn)

    if args.createTables:
        md.createTables(conn)

    psycopg2.extras.register_hstore(conn)

    if not (args.fileName is None):

        print 'parsing changeset file'
        changesetFile = None
        if(args.doReplication):
            changesetFile = gzip.open(args.fileName, 'rb')
        else:
            if(args.fileName[-4:] == '.bz2'):
                if(bz2Support):
                    changesetFile = BZ2File(args.fileName)
                else:
                    print 'ERROR: bzip2 support not available. Unzip file first or install bz2file'
                    sys.exit(1)
            else:
                changesetFile = open(args.fileName, 'rb')

        if(changesetFile != None):
            md.parseFile(conn, changesetFile, args.doReplication)
        else:
            print 'ERROR: no changeset file opened. Something went wrong in processing args'
            sys.exist(1)

        if(not args.doReplication):
            cursor = conn.cursor()
            print 'creating constraints'
            cursor.execute(queries.createConstraints)
            print 'creating indexes'
            cursor.execute(queries.createIndexes)

        conn.close()

    endTime = datetime.now()
    timeCost = endTime - beginTime

    print 'Processing time cost is ', timeCost

    print 'All done. Enjoy your (meta)data!'
