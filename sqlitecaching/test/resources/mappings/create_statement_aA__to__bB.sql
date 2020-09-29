-- sqlitecaching create table
CREATE TABLE IF NOT EXISTS "aa__bb"
(
    -- timestamp (for ordering)
    __timestamp TIMESTAMP,
    -- keys
    "a" A, -- primary key
    -- values
    "b" B, -- value
    PRIMARY KEY (
        "a"
    ) ON CONFLICT ABORT
);
