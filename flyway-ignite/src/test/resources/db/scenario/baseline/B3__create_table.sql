CREATE TABLE IF NOT EXISTS SecondTable (
   id UUID PRIMARY KEY,
   name VARCHAR NOT NULL,
   newColum LONG
) WITH "template=replicated,atomicity=atomic,cache_name=SecondTableCache";