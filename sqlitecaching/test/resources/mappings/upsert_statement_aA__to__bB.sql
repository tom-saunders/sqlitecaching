-- sqlitecaching insert or update into table
INSERT INTO "aa__bb"
(
    -- all columns
    "a", -- key
    "b" -- value
) VALUES (
    -- all values
    ?,
    ?
) ON CONFLICT (
    -- key columns
    "a" -- key
) DO UPDATE SET (
    -- value columns
    "b" -- value
) = (
    -- value values
    excluded."b" -- value
)
;
