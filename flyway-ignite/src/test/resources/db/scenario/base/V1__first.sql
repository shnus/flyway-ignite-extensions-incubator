CREATE TABLE IF NOT EXISTS Pet (
   id UUID PRIMARY KEY,
   name VARCHAR NOT NULL,
) WITH "template=replicated,atomicity=atomic,cache_name=PetCache";

CREATE INDEX IF NOT EXISTS pet_name_idx ON Pet (name);