-- sqlitecaching create table
CREATE TABLE IF NOT EXISTS "aa__c_"
(
    -- keys
    "a" A, -- primary key
    -- values
    "c" , -- value
    PRIMARY KEY (
        "a"
    ) ON CONFLICT ABORT
);
