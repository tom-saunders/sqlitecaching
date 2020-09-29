-- sqlitecaching retrieve from table
SELECT
    "c", -- value
    "d" -- value
FROM "aa_bb__cc_dd"
WHERE (
    -- key columns
    "a", -- key
    "b" -- key
) = (
    -- key values
    ?,
    ?
) ORDER BY __timestamp DESC;
