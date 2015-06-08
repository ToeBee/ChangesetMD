#!/usr/bin/python
'''
ChangesetMD is a simple XML parser to read the weekly changeset metadata dumps
from OpenStreetmap into a postgres database for querying.

@author: Toby Murray
'''
import os
import pwd
import sys
import argparse
import psycopg2
import psycopg2.extras
import queries
from lxml import etree
from datetime import datetime
from datetime import timedelta

try:
    from bz2file import BZ2File
    bz2Support = True
except ImportError:
    bz2Support = False

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

    def doIncremental(self, connection):
        """Prepare the table for incremental update and return the last changeset ID

        For incremental updates we delete all changesets that are newer than the oldest one
        marked as being open in the last dump. Then we skip all older changesets while parsing
        the new file to speed things up. This way we catch any changes that may have been made
        to open changesets after the last dump was made.
        """
        print 'preparing for incremental update'
        cursor = connection.cursor()
        cursor.execute(queries.deleteOpenChangesets)
        cursor.execute(queries.findNewestChangeset)
        return cursor.fetchone()[0]

    def insertNew(self, connection, id, userId, createdAt, minLat, maxLat, minLon, maxLon, closedAt, open, numChanges, userName, tags, comments):
        cursor = connection.cursor()
        cursor.execute('''INSERT into osm_changeset
                    (id, user_id, created_at, min_lat, max_lat, min_lon, max_lon, closed_at, open, num_changes, user_name, tags)
                    values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)''',
                    (id, userId, createdAt, minLat, maxLat, minLon, maxLon, closedAt, open, numChanges, userName, tags))
        for comment in comments:
            cursor.execute('''INSERT into osm_changeset_comment
                    (comment_note_id, comment_user_id, comment_user_name, comment_date, comment_text)
                    values (%s,%s,%s,%s,%s)''',
                    (id, comment['uid'], comment['user'], comment['date'], comment['text']))

    def parseFile(self, connection, newestChangeset, changesetFile):
        parsedCount = 0
        skippedCount = 0
        insertedCount = 0
        startTime = datetime.now()
        context = etree.iterparse(changesetFile)
        action, root = context.next()
        for action, elem in context:
            if(elem.tag != 'changeset'):
                continue

            parsedCount += 1
            if newestChangeset != -1 and long(elem.attrib['id']) <= newestChangeset:
                    skippedCount += 1
            else:
                tags = {}
                for tag in elem.iterchildren(tag='tag'):
                    tags[tag.attrib['k']] = tag.attrib['v']

                comments = []
                for discussion in elem.iterchildren(tag='discussion'):
                    comment = dict()
                    for commentElement in discussion.iterchildren(tag='comment'):
                        comment['uid'] = commentElement.attrib.get('uid')
                        comment['user'] = commentElement.attrib.get('user')
                        comment['date'] = commentElement.attrib.get('date')
                        for text in commentElement.iterchildren(tag='text'):
                            comment['text'] = text.text
                        comments.append(comment)

                self.insertNew(connection, elem.attrib['id'], elem.attrib.get('uid', None),
                               elem.attrib['created_at'], elem.attrib.get('min_lat', None),
                               elem.attrib.get('max_lat', None), elem.attrib.get('min_lon', None),
                               elem.attrib.get('max_lon', None),elem.attrib.get('closed_at', None),
                               elem.attrib.get('open', None), elem.attrib.get('num_changes', None),
                               elem.attrib.get('user', None), tags, comments)
                insertedCount += 1

            if((parsedCount % 10000) == 0):
                print "parsed %s skipped %s inserted %s" % ('{:,}'.format(parsedCount), '{:,}'.format(skippedCount), '{:,}'.format(insertedCount))
                print "cumulative rate: %s/sec" % '{:,.0f}'.format(parsedCount/timedelta.total_seconds(datetime.now() - startTime))
            
            #clear everything we don't need from memory to avoid leaking
            elem.clear()
            while elem.getprevious() is not None:
                del elem.getparent()[0]
        connection.commit()
        print "parsing complete"
        print "parsed {:,}".format(parsedCount)
        print "skipped {:,}".format(skippedCount)
        print "inserted {:,}".format(insertedCount)


if __name__ == '__main__':
    argParser = argparse.ArgumentParser(description="Parse OSM Changeset metadata into a database")
    argParser.add_argument('-t', '--trunc', action='store_true', default=False, dest='truncateTables', help='Truncate existing tables (also drops indexes)')
    argParser.add_argument('-c', '--create', action='store_true', default=False, dest='createTables', help='Create tables')
    argParser.add_argument('--host', action='store', dest='dbHost', help='Database hostname')
    argParser.add_argument('-u', '--user', action='store', dest='dbUser', default=pwd.getpwuid(os.getuid())[0], help='Database username (default: OS username)')
    argParser.add_argument('-p', '--password', action='store', dest='dbPass', default='', help='Database password (default: blank)')
    argParser.add_argument('-d', '--database', action='store', dest='dbName', help='Target database', required=True)
    argParser.add_argument('-f', '--file', action='store', dest='fileName', help='OSM changeset file to parse')
    argParser.add_argument('-i', '--incremental', action='store_true', default=False, dest='incrementalUpdate', help='Perform incremental update. Only import new changesets')
    args = argParser.parse_args()

    if not (args.dbHost is None):
        conn = psycopg2.connect(database=args.dbName, user=args.dbUser, password=args.dbPass, host=args.dbHost)
    else:
        conn = psycopg2.connect(database=args.dbName, user=args.dbUser, password=args.dbPass)

    md = ChangesetMD()
    if args.truncateTables:
        md.truncateTables(conn)

    if args.createTables:
        md.createTables(conn)

    newestChangeset = -1
    if args.incrementalUpdate:
        newestChangeset = md.doIncremental(conn)
        print "Performing incremental update from changeset {:,}".format(newestChangeset)

    psycopg2.extras.register_hstore(conn)

    if not (args.fileName is None):

        print 'parsing changeset file'
        changesetFile = None
        if(args.fileName[-4:] == '.bz2'):
            if(bz2Support):
                md.parseFile(conn, newestChangeset, BZ2File(args.fileName))
            else:
                print 'ERROR: bzip2 support not available. Unzip file first or install bz2file'
                sys.exit(1)
        else:
            md.parseFile(conn, newestChangeset, open(args.fileName, 'r'))

        if not args.incrementalUpdate:
            cursor = conn.cursor()
            print 'creating constraints'
            cursor.execute(queries.createConstraints)
            print 'creating indexes'
            cursor.execute(queries.createIndexes)

        conn.close()

    print 'All done. Enjoy your (meta)data!'
