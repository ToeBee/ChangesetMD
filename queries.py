'''
Just a utility file to store some SQL queries for easy reference

@author: Toby Murray
'''
createChangesetTable = '''CREATE EXTENSION hstore;
  CREATE TABLE osm_changeset (
  id bigint,
  user_id bigint,
  created_at timestamp without time zone,
  min_lat numeric(10,7),
  max_lat numeric(10,7),
  min_lon numeric(10,7),
  max_lon numeric(10,7),
  closed_at timestamp without time zone,
  num_changes integer,
  user_name varchar(255),
  tags hstore
)
'''

dropIndexes = '''ALTER TABLE osm_changeset DROP CONSTRAINT IF EXISTS osm_changeset_pkey CASCADE;
DROP INDEX IF EXISTS tags_key_idx, tags_value_idx;
DROP INDEX IF EXISTS user_name_idx, user_id_idx, created_idx;
'''

createConstraints = '''ALTER TABLE osm_changeset ADD CONSTRAINT osm_changeset_pkey PRIMARY KEY(id);'''

createIndexes = '''CREATE INDEX user_name_idx ON osm_changeset(user_name);
CREATE INDEX user_id_idx ON osm_changeset(user_id);
CREATE INDEX created_idx ON osm_changeset(created_at);
CREATE INDEX tags_idx ON osm_changeset USING GIN(tags);
'''

