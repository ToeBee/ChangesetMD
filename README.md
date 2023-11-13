ChangesetMD
=========

ChangesetMD is a simple XML parser written in python that takes the weekly changeset metadata dump file from http://planet.openstreetmap.org/ and shoves the data into a simple postgres database so it can be queried.

It can also keep a database created with a weekly dump file up to date using minutely changeset diff files available at [http://planet.osm.org/replication/changesets/](http://planet.osm.org/replication/changesets/)

Setup
------------

ChangesetMD works with Python 3.6 or newer.

Aside from postgresql, ChangesetMD depends on the python libraries psycopg2 and lxml.
On Debian-based systems this means installing the python-psycopg2 and python-lxml packages.

If you are using `pip` and `virtualenv`, you can install all dependencies with `pip install -r requirements.txt`.

If you want to parse the changeset file without first unzipping it, you will also need to install the [bz2file library](http://pypi.python.org/pypi/bz2file) since the built in bz2 library can not handle multi-stream bzip files.

For building geometries, ```postgis``` extension needs to be [installed](http://postgis.net/install).

ChangesetMD expects a postgres database to be set up for it. It can likely co-exist within another database if desired. Otherwise, As the postgres user execute:

    createdb changesets

It is easiest if your OS user has access to this database. I just created a user and made myself a superuser. Probably not best practices.

    createuser <username>


Full Debian build instructions
------------------------------

    sudo apt install sudo screen locate git tar unzip wget bzip2 apache2 python3-psycopg2 python3-yaml libpq-dev postgresql postgresql-contrib postgis postgresql-15-postgis-3 postgresql-15-postgis-3-scripts net-tools curl python3-full gcc libpython3.11-dev libxml2-dev libxslt-dev

    python3 -m venv .venv
    source .venv/bin/activate
    pip install -r requirements.txt

    sudo -u postgres -i
    createuser youruseraccount
    createdb -E UTF8 -O youruseraccount changesets

    psql
    \c changesets
    CREATE EXTENSION postgis;
    ALTER TABLE geometry_columns OWNER TO youruseraccount;
    ALTER TABLE spatial_ref_sys OWNER TO youruseraccount;
    \q
    exit


Execution
------------
The first time you run it, you will need to include the -c | --create option to create the table:

    python changesetmd.py -d <database> -c -g

The `-g` | `--geometry` argument is optional and builds polygon geometries for changesets so that you can query which changesets were within which areas.

The create function can be combined with the file option to immediately parse a file.

To parse a dump file, use the -f | --file option.

    python changesetmd.py -d <database> -g -f /tmp/discussions-latest.osm.bz2

If no other arguments are given, it will access postgres using the default settings of the postgres client, typically connecting on the unix socket as the current OS user. Use the ```--help``` argument to see optional arguments for connecting to postgres.

Again, the `-g` | `--geometry` argument is optional.  Either of changeset-latest.osm.bz2 or discussions-latest.osm.bz2 or neither can be used to populate the database.

Replication
------------
After you have parsed a weekly dump file into the database, the database can be kept up to date using changeset diff files that are generated on the OpenStreetMap planet server every minute. To initiate the replication system you will need to find out which minutely sequence number you need to start with and update the ```osm_changeset_state``` table so that ChangesetMD knows where to start. Unfortunately there isn't an easy way to get the needed sequence number from the dump file. Here is the process to find it:

First, determine the timestamp present in the first line of XML in the dump file. Assuming you are starting from the .bzip2 file, use this command:

    bunzip2 -c discussions-latest.osm.bz2 | head

Look for this line:

    <osm license="http://opendatacommons.org/licenses/odbl/1-0/" copyright="OpenStreetMap and contributors" version="0.6" generator="planet-dump-ng 1.1.2" attribution="http://www.openstreetmap.org/copyright" timestamp="2015-11-16T01:59:54Z">

Note the timestamp at the end of it. In this case, just before 02:00 on November 16th, 2015. Now browse to [http://planet.osm.org/replication/changesets/](http://planet.osm.org/replication/changesets/) and navigate the directories until you find files with a similar timestamp as the one from the dump file. Each second level directory contains 1,000 diffs so there is generally one directory per day with one day occasionally crossing two directories.

Unfortunately there is no metadata file that goes along with the changeset diff files (like there is with the map data diff files) so there isn't a way to narrow it down to one specific file. However it is safe to apply older diffs to the database since it will just update the data to its current state again. So just go back 2 or 3 hours from the timestamp in the dump file and start there. This will ensure that any time zone setting or daylight savings time will be accounted for. So in the example from above, look for the file with a timestamp around November 15th at 23:00 since that is 3 hours before the given timestamp in the dump file of 02:00 on November 16th.

This gives the file 048.osm.gz in the directory [http://planet.osm.org/replication/changesets/001/582/](http://planet.osm.org/replication/changesets/001/582/). Now take the numbers of all the directories and the file and remove the slashes. So 001/582/048.osm.gz becomes: 1582048. This is the sequence to start replication at. To set this, run the following SQL query in postgres:

    update osm_changeset_state set last_sequence = 1582048;

Now you are ready to start consuming the replication diffs with the following command:

    python changesetmd.py -d <database> -r

Run this command as often as you wish to keep your database up to date with OSM. You can put it in a cron job that runs every minute if you like. The first run may take a few minutes to catch up but each subsequent run should only take a few seconds to finish.

Notes
------------
- Prints a status message every 10,000 records.
- Takes 2-3 hours to import the current dump on a decent home computer.
- Might be faster to process the XML into a flat file and then use the postgres COPY command to do a bulk load but this would make incremental updates a little harder
- I have commonly queried fields indexed. Depending on what you want to do, you may need more indexes.
- Changesets can be huge in extent, so you may wish to filter them by area before any visualization. 225 square km seems to be a fairly decent threshold to get the actual spatial footprint of edits. `WHERE ST_Area(ST_Transform(geom, 3410)) < 225000000` will do the trick.
- Some changesets have bounding latitudes outside the range of [-90;90] range. Make sure you handle them right before projecting (e.g. for area checks).

Table Structure
------------
ChangesetMD populates two tables with the following structure:

osm\_changeset:  
Primary table of all changesets with the following columns:
- `id`: changeset ID
- `created_at/closed_at`: create/closed time 
- `num_changes`: number of objects changed
- `min_lat/max_lat/min_lon/max_lon`: description of the changeset bbox in decimal degrees
- `user_name`: OSM username
- `user_id`: numeric OSM user ID
- `tags`: an hstore column holding all the tags of the changeset
- `geom`: [optional] a postgis geometry column of `Polygon` type (SRID: 4326)

Note that all fields except for id and created\_at can be null.

osm\_changeset\_comment:
All comments made on changesets via the new commenting system
- `comment_changeset_id`: Foreign key to the changeset ID
- `comment_user_id`: numeric OSM user ID
- `comment_user_name`: OSM username
- `comment_date`: timestamp of when the comment was created

If you are unfamiliar with hstore and how to query it, see the [postgres documentation](http://www.postgresql.org/docs/9.2/static/hstore.html)

Example queries
------------
Count how many changesets have a comment tag:

    SELECT COUNT(*)
    FROM osm_changeset
    WHERE tags ? 'comment';

Find all changesets that were created by JOSM:

    SELECT COUNT(*)
    FROM osm_changeset
    WHERE tags -> 'created_by' LIKE 'JOSM%';

Find all changesets that were created in Liberty Island:

    SELECT count(id)
    FROM osm_changeset c, (SELECT ST_SetSRID(ST_MakeEnvelope(-74.0474545,40.6884971,-74.0433990,40.6911817),4326) AS geom) s
    WHERE ST_CoveredBy(c.geom, s.geom);
