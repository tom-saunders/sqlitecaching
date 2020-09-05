-- sqlitecaching create table
CREATE TABLE aa_bb
(
    -- keys
    'a' A, -- primary key
    'b' B, -- primary key

    -- values

    PRIMARY KEY (
        'a',
        'b'
    ) ON CONFLICT ABORT
);
