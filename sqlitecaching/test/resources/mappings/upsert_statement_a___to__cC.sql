-- sqlitecaching insert or update into table
INSERT INTO "a___cc"
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
    -- timestamp
    __timestamp,
    -- value columns
    "c" -- value
) = (
    -- timestamp
    excluded.__timestamp,
    -- value values
    excluded."c" -- value
)
;
