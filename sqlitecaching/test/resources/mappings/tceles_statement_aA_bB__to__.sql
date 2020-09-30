-- sqlitecaching retrieve from table
SELECT
    __timestamp -- no value columns so just __timestamp
FROM "aa_bb"
WHERE (
    -- key columns
    "a", -- key
    "b" -- key
) = (
    -- key values
    ?,
    ?
) ORDER BY __timestamp DESC;
