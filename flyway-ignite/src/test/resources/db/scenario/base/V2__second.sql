CREATE TABLE IF NOT EXISTS Owner (
   id UUID PRIMARY KEY,
   ownerId UUID NOT NULL,
) WITH "template=replicated,atomicity=atomic,cache_name=OwnerCache";

CREATE INDEX IF NOT EXISTS owner_owner_id_idx ON Owner (ownerId);