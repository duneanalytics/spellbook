CREATE SCHEMA IF NOT EXISTS setprotocol_v2
;

create or replace view setprotocol_v2.set_protocol_all_sets as
select distinct "_setToken" as set_address
    , "_name" as set_name
    , "_symbol" as set_symbol
from setprotocol_v2."SetTokenCreator_evt_SetTokenCreated"
;