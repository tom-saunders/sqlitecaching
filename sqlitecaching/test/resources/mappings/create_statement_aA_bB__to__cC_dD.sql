-- sqlitecaching create table
CREATE TABLE IF NOT EXISTS "aa_bb__cc_dd"
(
    -- timestamp (for ordering)
    __timestamp TIMESTAMP,
    -- keys
    "a" A, -- primary key
    "b" B, -- primary key
    -- values
    "c" C, -- value
    "d" D, -- value
    PRIMARY KEY (
        "a",
        "b"
    ) ON CONFLICT ABORT
);
