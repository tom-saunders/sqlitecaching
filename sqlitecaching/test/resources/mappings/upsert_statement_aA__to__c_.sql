-- sqlitecaching insert or update into table
INSERT INTO "aa__c_"
(
    -- timestamp
    __timestamp,
    -- all columns
    "a", -- key
    "c" -- value
) VALUES (
    -- timestamp
    ?,
    -- all values
    ?,
    ?
) ON CONFLICT (
    -- key columns
    "a" -- key
) DO UPDATE SET (
    -- value columns
    "c" -- value
) = (
    -- value values
    excluded."c" -- value
)
;
