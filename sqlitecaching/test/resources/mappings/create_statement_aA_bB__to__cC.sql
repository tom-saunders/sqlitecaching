-- sqlitecaching create table
CREATE TABLE 'aa_bb__cc'
(
    -- keys
    'a' A, -- primary key
    'b' B, -- primary key
    -- values
    'c' C, -- value
    PRIMARY KEY (
        'a',
        'b'
    ) ON CONFLICT ABORT
);
