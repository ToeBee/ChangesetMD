'''
Just a utility file to store some SQL queries for easy reference

@author: Toby Murray
'''
createChangesetTable = '''CREATE TABLE osm_changeset ( 
  id bigint,
  user_id bigint,
  created_at timestamp without time zone,
  min_lat numeric(10,7),
  max_lat numeric(10,7),
  min_lon numeric(10,7),
  max_lon numeric(10,7),
  closed_at timestamp without time zone,
  num_changes integer,
  user_name varchar(255)
)
'''

createTagsTable = '''CREATE TABLE osm_changeset_tags (
  changeset_id bigint,
  key varchar(255),
  value varchar(255)
)'''

dropIndexes = '''ALTER TABLE osm_changeset DROP CONSTRAINT IF EXISTS osm_changeset_pkey CASCADE;
DROP INDEX IF EXISTS tags_key_idx, tags_value_idx;
DROP INDEX IF EXISTS user_name_idx
'''

createConstraints = '''ALTER TABLE osm_changeset ADD CONSTRAINT osm_changeset_pkey PRIMARY KEY(id);
ALTER TABLE osm_changeset_tags ADD CONSTRAINT osm_changeset_tags_fk FOREIGN KEY (changeset_id) REFERENCES osm_changeset(id);
'''

createIndexes = '''CREATE INDEX on osm_changeset_tags(key);
CREATE INDEX tags_key_idx ON osm_changeset_tags(value);
CREATE INDEX tags_value_idx ON osm_changeset_tags(changeset_id);
CREATE INDEX user_name_idx ON osm_changeset(user_name);
'''

