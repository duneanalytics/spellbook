CREATE TABLE IF NOT EXISTS gnosis_protocol_v2.named_solvers
(
    address     text,
    environment text,
    name        text
);

COPY gnosis_protocol_v2.named_solvers (address, environment, name)
    FROM '/ethereum/gnosis_protocol_v2/solver_names.csv'
    DELIMITER ','
    CSV HEADER;

CREATE UNIQUE INDEX IF NOT EXISTS solver_id ON gnosis_protocol_v2.named_solvers (address);

