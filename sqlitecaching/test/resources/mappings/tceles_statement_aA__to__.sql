-- sqlitecaching retrieve from table
SELECT
    __timestamp -- no value columns so just __timestamp
FROM "aa"
WHERE (
    -- key columns
    "a" -- key
) = (
    -- key values
    ?
) ORDER BY __timestamp DESC;
