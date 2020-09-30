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
    -- value columns
    "b" -- value
) = (
    -- value values
    excluded."b" -- value
)
;
