CREATE TABLE IF NOT EXISTS FirstTable (
   id UUID PRIMARY KEY,
   name VARCHAR NOT NULL,
) WITH "template=replicated,atomicity=atomic,cache_name=FirstTableCache";

CREATE TABLE IF NOT EXISTS SecondTable (
   id UUID PRIMARY KEY,
   name VARCHAR NOT NULL,
) WITH "template=replicated,atomicity=atomic,cache_name=SecondTableCache";