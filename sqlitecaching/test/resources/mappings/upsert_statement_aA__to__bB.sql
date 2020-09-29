-- sqlitecaching insert or update into table
INSERT INTO "aa__bb"
(
    -- timestamp
    __timestamp,
    -- all columns
    "a", -- key
    "b" -- value
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
    -- timestamp
    __timestamp,
    -- value columns
    "b" -- value
) = (
    -- timestamp
    excluded.__timestamp,
    -- value values
    excluded."b" -- value
)
;
