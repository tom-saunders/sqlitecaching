-- sqlitecaching insert or update into table
INSERT INTO "aa_bb__cc_dd"
(
    -- timestamp
    __timestamp,
    -- all columns
    "a", -- key
    "b", -- key
    "c", -- value
    "d" -- value
) VALUES (
    -- timestamp
    ?,
    -- all values
    ?,
    ?,
    ?,
    ?
) ON CONFLICT (
    -- key columns
    "a", -- key
    "b" -- key
) DO UPDATE SET (
    -- timestamp
    __timestamp,
    -- value columns
    "c", -- value
    "d" -- value
) = (
    -- timestamp
    excluded.__timestamp,
    -- value values
    excluded."c", -- value
    excluded."d" -- value
)
;
