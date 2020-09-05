-- sqlitecaching remove from table
DELETE FROM 'aa_bb'
WHERE (
    -- key_columns
    'a', -- key
    'b' -- key
) = (
    -- key_values
    ?,
    ?,
);
