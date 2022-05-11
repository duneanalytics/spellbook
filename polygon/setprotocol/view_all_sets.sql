-- drop view if exists dune_user_generated.set_protocol_all_sets;

create or replace view setprotocol.view_all_sets as

select distinct "_setToken" as set_address
    , "_name" as set_name
    , "_symbol" as set_symbol
from setprotocol."SetTokenCreator_evt_SetTokenCreated"
;