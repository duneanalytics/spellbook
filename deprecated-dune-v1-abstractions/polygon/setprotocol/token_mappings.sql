-- this is an empty placeholder table that can take 1:1 token mappings in the future
-- See the ethereum setprotocol_v2 schema

create table if not exists setprotocol.token_mappings
(
  symbol varchar,
  component_address bytea,
  token_type varchar,
  mapped_symbol varchar,
  mapped_component_address bytea         
);