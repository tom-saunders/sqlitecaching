-- sqlitecaching create table
CREATE TABLE IF NOT EXISTS "aa"
(
    -- keys
    "a" A, -- primary key
    -- values
    -- no values defined
    PRIMARY KEY (
        "a"
    ) ON CONFLICT ABORT
);
