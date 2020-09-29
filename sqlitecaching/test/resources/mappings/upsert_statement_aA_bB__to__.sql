-- sqlitecaching insert or update into table
INSERT INTO "aa_bb"
(
    -- timestamp
    __timestamp,
    -- all columns
    "a", -- key
    "b" -- key
    -- no values defined
) VALUES (
    -- timestamp
    ?,
    -- all values
    ?,
    ?
) ON CONFLICT (
    -- key columns
    "a", -- key
    "b" -- key
) DO UPDATE SET (
    -- timestamp
    __timestamp
) = (
    -- timestamp
    excluded.__timestamp
)
;
