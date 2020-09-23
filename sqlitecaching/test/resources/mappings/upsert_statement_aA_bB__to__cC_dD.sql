-- sqlitecaching insert or update into table
INSERT INTO "aa_bb__cc_dd"
(
    -- all columns
    "a", -- key
    "b", -- key
    "c", -- value
    "d" -- value
) VALUES (
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
    -- value columns
    "c", -- value
    "d" -- value
) = (
    -- value values
    excluded."c", -- value
    excluded."d" -- value
)
;
