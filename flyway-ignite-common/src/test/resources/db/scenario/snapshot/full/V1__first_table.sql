CREATE TABLE IF NOT EXISTS FirstTable (
   id UUID PRIMARY KEY,
   name VARCHAR NOT NULL,
) WITH "template=replicated,atomicity=atomic,cache_name=FirstTableCache";

CREATE INDEX IF NOT EXISTS first_table_name_idx ON FirstTable (name);