-- sqlitecaching create table
CREATE TABLE IF NOT EXISTS "a___cc"
(
    -- timestamp (for ordering)
    __timestamp TIMESTAMP,
    -- keys
    "a" , -- primary key
    -- values
    "c" C, -- value
    PRIMARY KEY (
        "a"
    ) ON CONFLICT ABORT
);
