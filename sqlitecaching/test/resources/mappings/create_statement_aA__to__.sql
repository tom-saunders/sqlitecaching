-- sqlitecaching create table
CREATE TABLE IF NOT EXISTS "aa"
(
    -- timestamp (for ordering)
    __timestamp TIMESTAMP,
    -- keys
    "a" A, -- primary key
    -- values
    -- no values defined
    PRIMARY KEY (
        "a"
    ) ON CONFLICT ABORT
);
