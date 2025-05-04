-- DELETE FROM sf_temp;

-- INSERT INTO sf_temp (datetime, temperature) VALUES ('2012-10-01 07:00:00-07', '16.281869');

-- SELECT * FROM sf_temp;
SELECT datetime::timestamptz AT TIME ZONE 'America/Los_Angeles', temperature FROM sf_temp;