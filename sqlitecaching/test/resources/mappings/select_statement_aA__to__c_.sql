-- sqlitecaching retrieve from table
SELECT
    "c" -- value
FROM "aa__c_"
WHERE (
    -- key columns
    "a" -- key
) = (
    -- key values
    ?
) ORDER BY __timestamp ASC;
