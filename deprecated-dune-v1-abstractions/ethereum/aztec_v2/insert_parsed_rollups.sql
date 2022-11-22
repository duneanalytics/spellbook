
-- This insert query is designed to check for non-parsed rollup txns and parse them and then insert them
CREATE OR REPLACE FUNCTION aztec_v2.insert_parsed_rollups() RETURNS integer
-- CREATE OR REPLACE FUNCTION dune_user_generated.insert_parsed_rollups() RETURNS integer
LANGUAGE plpgsql AS $function$
DECLARE r integer;
BEGIN
with rows as (
  -- insert into dune_user_generated.aztec_v2_rollups_parsed_cached ( 
  insert into aztec_v2.rollups_parsed ( 
    call_block_time 
    , contract_address 
    , call_tx_hash 
    , rollupid 
    , rollupsize 
    , datastartindex 
    , olddataroot 
    , newdataroot 
    , oldnullroot 
    , newnullroot 
    , olddatarootsroot 
    , newdatarootsroot 
    , olddefiroot 
    , newdefiroot 
    , bridges 
    , defidepositsums 
    , assetids 
    , totaltxfees 
    , defiinteractionnotes 
    , prevdefiinteractionhash 
    , rollupbeneficiary 
    , numrolluptxs 
    , innerproofs 
  )
  select
    r."call_block_time"
    , r.contract_address
    , r.call_tx_hash
    , (aztec_v2.fn_process_aztec_block(r."_0")).*
  from aztec_v2."RollupProcessor_call_processRollup" r
  -- left join dune_user_generated.aztec_v2_rollups_parsed_cached rp on r.call_tx_hash = rp.call_tx_hash
  left join aztec_v2.rollups_parsed rp on r.call_tx_hash = rp.call_tx_hash
  where rp.call_tx_hash is null -- grab rollup txns that haven't been parsed yet
  returning 1
)
select count(*) into r
from rows;
return r;
end
$function$;

-- backfill the transactions
select aztec_v2.insert_parsed_rollups();

-- check for non-parsed rollup blocks once every 15 minutes
INSERT INTO cron.job (schedule, command)
VALUES ('0,15,30,45 * * * *', $$
    SELECT aztec_v2.insert_parsed_rollups()
$$)
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;