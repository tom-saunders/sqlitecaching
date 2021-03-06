-- sqlitecaching create table
CREATE TABLE IF NOT EXISTS "aa_bb"
(
    -- timestamp (for ordering)
    __timestamp TIMESTAMP,
    -- keys
    "a" A, -- primary key
    "b" B, -- primary key
    -- values
    -- no values defined
    PRIMARY KEY (
        "a",
        "b"
    ) ON CONFLICT ABORT
);
