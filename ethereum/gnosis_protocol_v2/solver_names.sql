CREATE TABLE IF NOT EXISTS gnosis_protocol_v2.solver_names
(
    address     bytea,
    environment text,
    name        text
);

\copy gnosis_protocol_v2.solver_names FROM PROGRAM 'curl https://raw.githubusercontent.com/duneanalytics/abstractions/master/ethereum/gnosis_protocol_v2/solver_names.csv' with (format csv,header true, delimiter ',');

CREATE UNIQUE INDEX IF NOT EXISTS solver_id ON gnosis_protocol_v2.solver_names (address);
