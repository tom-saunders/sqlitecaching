-- sqlitecaching retrieve from table
SELECT (
    'c' -- value
) FROM 'aa_bb__cc'
WHERE (
    -- key columns
    'a', -- key
    'b' -- key
) = (
    -- key values
    ?,
    ?
);
