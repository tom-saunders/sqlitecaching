-- sqlitecaching insert or update into table
INSERT INTO 'aa_bb'
(
    -- all columns
    'a', -- key
    'b' -- key
    -- no values defined
) VALUES (
    -- all values
    ?,
    ?
) ON CONFLICT DO NOTHING
-- no conflict action as no values defined
;
