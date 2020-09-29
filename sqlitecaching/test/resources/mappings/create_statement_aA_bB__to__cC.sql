-- sqlitecaching create table
CREATE TABLE IF NOT EXISTS "aa_bb__cc"
(
    -- timestamp (for ordering)
    __timestamp TIMESTAMP,
    -- keys
    "a" A, -- primary key
    "b" B, -- primary key
    -- values
    "c" C, -- value
    PRIMARY KEY (
        "a",
        "b"
    ) ON CONFLICT ABORT
);
