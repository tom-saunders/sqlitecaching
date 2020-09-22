-- sqlitecaching retrieve from table
SELECT (
    null -- no value columns so use null
) FROM 'aa_bb'
WHERE (
    -- key columns
    'a', -- key
    'b' -- key
) = (
    -- key values
    ?,
    ?
);
