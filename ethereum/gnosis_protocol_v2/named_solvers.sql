CREATE TABLE IF NOT EXISTS gnosis_protocol_v2.named_solvers
(
    address     text,
    environment text,
    name        text
);

-- COPY gnosis_protocol_v2.named_solvers (address, environment, name)
--     FROM 'https://raw.githubusercontent.com/duneanalytics/abstractions/a68ea6d2c3045d02c29c974556d0d230533a1263/ethereum/gnosis_protocol_v2/solvers.csv'
--     DELIMITER ','
--     CSV HEADER;

\copy gnosis_protocol_v2.named_solvers FROM 'https://raw.githubusercontent.com/duneanalytics/abstractions/a68ea6d2c3045d02c29c974556d0d230533a1263/ethereum/gnosis_protocol_v2/solvers.csv' with (format csv,header true, delimiter ',');

CREATE UNIQUE INDEX IF NOT EXISTS solver_id ON gnosis_protocol_v2.named_solvers (address);

