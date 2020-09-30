-- sqlitecaching insert or update into table
INSERT INTO "aa"
(
    -- timestamp
    __timestamp,
    -- all columns
    "a" -- key
    -- no values defined
) VALUES (
    -- timestamp
    ?,
    -- all values
    ?
) ON CONFLICT DO NOTHING
;
