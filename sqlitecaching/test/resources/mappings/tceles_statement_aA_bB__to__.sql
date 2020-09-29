-- sqlitecaching retrieve from table
SELECT
    NULL -- no value columns so just NULL
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
