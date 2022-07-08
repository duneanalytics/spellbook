CREATE SCHEMA IF NOT EXISTS balancer;

CREATE TABLE IF NOT EXISTS balancer.address_mappings (
    address bytea NOT NULL,
    label   text  NOT NULL,
    type    text  NOT NULL,
    author  text  NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS balancer_address_label_uniq_idx ON balancer.address_mappings (address, label);
CREATE INDEX IF NOT EXISTS balancer_address_idx ON balancer.address_mappings (address);
