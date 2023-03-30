-- Create a single table for test purpose

CREATE TABLE test_table (
   time             TIMESTAMPTZ                 NOT NULL,
   vessel_id        TEXT                        NOT NULL,
   parameter_id     TEXT                        NOT NULL,
   value            TEXT                        NOT NULL
);