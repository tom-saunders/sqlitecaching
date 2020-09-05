-- sqlitecaching create table
CREATE TABLE 'aa__bb'
(
    -- keys
    'a' A, -- primary key
    -- values
    'b' B, -- value
    PRIMARY KEY (
        'a'
    ) ON CONFLICT ABORT
);
