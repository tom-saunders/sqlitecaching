-- sqlitecaching remove from table
DELETE FROM "aa_bb__cc_dd"
WHERE (
    -- key columns
    "a", -- key
    "b" -- key
) = (
    -- key values
    ?,
    ?
);
