-- sqlitecaching retrieve from table
SELECT
    null -- no value columns so just null
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
