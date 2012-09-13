#!/usr/bin/python
'''
ChangesetMD is a simple XML parser to read the weekly changeset metadata dumps
from OpenStreetmap into a postgres database for querying.

@author: Toby Murray
'''
import os, pwd
import argparse
import xml.sax
import psycopg2
import psycopg2.extras
import changesethandler
import queries



class ChangesetMD():
    def truncateTables(self, connection):
        print 'truncating tables'
        cursor = connection.cursor()
        cursor.execute("TRUNCATE TABLE osm_changeset CASCADE")
        cursor.execute(queries.dropIndexes)
        
    def createTables(self, connection):
        print 'creating tables'
        cursor = connection.cursor()
        cursor.execute(queries.createChangesetTable)
        connection.commit()

if __name__ == '__main__':
    argParser = argparse.ArgumentParser(description="Parse OSM Changeset metadata into a database")
    argParser.add_argument('-t', '--trunc', action='store_true', default=False, dest='truncateTables', help='Truncate existing tables (also drops indexes)')
    argParser.add_argument('-c', '--create', action='store_true', default=False, dest='createTables', help='Create tables')
    argParser.add_argument( '--host', action='store', dest='dbHost', help='Database hostname')
    argParser.add_argument('-u', '--user', action='store', dest='dbUser', default=pwd.getpwuid(os.getuid())[0], help='Database username (default: OS username)')
    argParser.add_argument('-p', '--password', action='store', dest='dbPass', default='', help='Database password (default: blank)')
    argParser.add_argument('-d', '--database', action='store', dest='dbName', help='Target database', required=True)
    argParser.add_argument('-f', '--file', action='store', dest='fileName', help='OSM changeset file to parse')
    args = argParser.parse_args()
    
    if not (args.dbHost is None):
        conn = psycopg2.connect(database=args.dbName, user=args.dbUser, password=args.dbPass, host=args.dbHost)
    else:
        conn = psycopg2.connect(database=args.dbName, user=args.dbUser, password=args.dbPass)
    
    foo = ChangesetMD()
    if args.truncateTables:
        foo.truncateTables(conn)
        
    if args.createTables:
        foo.createTables(conn)

    psycopg2.extras.register_hstore(conn)
        
    if not (args.fileName is None):
        parser = xml.sax.make_parser()
        handler = changesethandler.ChangesetHandler(conn)
        parser.setContentHandler(handler)
        print 'parsing changeset file'
        parser.parse(args.fileName)
        cursor = conn.cursor()
        print 'creating constraints'
        cursor.execute(queries.createConstraints)
        print 'creating indexes'
        cursor.execute(queries.createIndexes)
        conn.commit()
        conn.close()
    
    print 'All done. Enjoy your (meta)data!'
    

        