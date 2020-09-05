-- sqlitecaching remove from table
DELETE FROM 'aa_bb'
WHERE (
    -- key columns
    'a', -- key
    'b' -- key
) = (
    -- key values
    ?,
    ?
);
