-- sqlitecaching remove from table
DELETE FROM "aa_bb__cc"
WHERE (
    -- key columns
    "a", -- key
    "b" -- key
) = (
    -- key values
    ?,
    ?
);
