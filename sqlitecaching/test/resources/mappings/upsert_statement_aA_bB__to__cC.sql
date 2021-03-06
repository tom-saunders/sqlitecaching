-- sqlitecaching insert or update into table
INSERT INTO "aa_bb__cc"
(
    -- timestamp
    __timestamp,
    -- all columns
    "a", -- key
    "b", -- key
    "c" -- value
) VALUES (
    -- timestamp
    ?,
    -- all values
    ?,
    ?,
    ?
) ON CONFLICT (
    -- key columns
    "a", -- key
    "b" -- key
) DO UPDATE SET (
    -- value columns
    "c" -- value
) = (
    -- value values
    excluded."c" -- value
)
;
