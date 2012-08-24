ChangesetMD
=========

ChangesetMD is a simple XML parser written in python that takes the weekly changeset metadata dump file from http://planet.openstreetmap.org/ and shoves the data into a simple postgres database so it can be queried.

WARNING: This is pretty much my first python project ever beyond "hello world" so... you have been warned.


Setup
------------

ChangesetMD works with python 2.7.

Aside from postgresql, ChangesetMD depends on the python librarys psycopg2 and dateutil.
On Debian-based systems this means installing the python-psycopg2 and python-dateutil packages.

ChangesetMD expects a postgres database to be set up for it. It can likely co-exist within another database if desired. Otherwise, As the postgres user execute:

    createdb changesets

It is easiest if your OS user has access to this database. I just created a user and made myself a superuser. Probably not best practices.

    createuser <username>


Execution
------------
The first time you run it, you will need to include the -c | --create option to create the two tables:

    python changesetmd.py -d <database> -c

The create function can be combined with the file option to immediately parse a file.

To parse the file, use the -f | --file option. After the first run to create the tables, you can use -t | --truncate to clear out the tables and import a new file:

    python changesetmd.py -d <database> -t -f /tmp/changeset-latest.osm

Optional database user/password/host arguments can be used to access a postgres database in other ways.


Notes
------------
- Does not currently support reading directly from .bz2 files. Unzip them first.
- Prints a message every 10,000 records.
- Takes about 4 hours to import the current dump on a decent home computer.
- Would likely be faster to process the XML into two flat files and then use the postgres COPY command to do a bulk load
- Needs more indexes to make querying practical. I'm waiting on a first full load to experiment with indexes


Table Structure
------------
ChangesetMD populates two tables

osm\_changeset:

- `id`: changeset ID
- `created_at/closed_at`: create/closed time 
- `num_changes`: number of objects changed
- `min_lat/max_lat/min_lon/max_lon`: description of the changeset bbox in decimal degrees
- `user_name`: OSM username
- `user_id`: numeric OSM user ID

Note that all fields except for ID and created time can be null.

Changeset tags are in their own table since there may be an arbitrary number of them.

osm\_changeset\_tags:

- `changeset_id`: changeset ID, foreign key to osm\_changeset
- `key`: tag key
- `value`: tag value

Example query: count how many changesets have a created\_by=\* tag.

    select count(*) 
    from osm_changeset, osm_changeset_tags 
    where changeset_id = id and key = 'created_by';


License
------------
Copyright (C) 2012  Toby Murray

This program is free software: you can redistribute it and/or modify it under the terms of the GNU Affero General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  

See the GNU Affero General Public License for more details: http://www.gnu.org/licenses/agpl.txt
