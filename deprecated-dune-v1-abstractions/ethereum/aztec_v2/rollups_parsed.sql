-- CREATE TABLE IF NOT EXISTS dune_user_generated.aztec_v2_rollups_parsed_cached(
CREATE TABLE IF NOT EXISTS aztec_v2.rollups_parsed ( 
  call_block_time timestamp with time zone
  , contract_address bytea
  , call_tx_hash bytea
  , rollupid numeric
  , rollupsize numeric
  , datastartindex numeric
  , olddataroot bytea
  , newdataroot bytea
  , oldnullroot bytea
  , newnullroot bytea
  , olddatarootsroot bytea
  , newdatarootsroot bytea
  , olddefiroot bytea
  , newdefiroot bytea
  -- , bridges dune_user_generated.aztec_v2_proof_bridge_data_struct []
  , bridges aztec_v2.proof_bridge_data_struct []
  , defidepositsums numeric[]
  , assetids numeric[]
  , totaltxfees numeric []
  , defiinteractionnotes bytea []
  , prevdefiinteractionhash bytea
  , rollupbeneficiary bytea
  , numrolluptxs numeric
  -- , innerproofs dune_user_generated.aztec_v2_inner_proof_data_struct [] 
  , innerproofs aztec_v2.inner_proof_data_struct [] 
);

CREATE UNIQUE INDEX IF NOT EXISTS aztec_v2_rollups_parsed_uniq_idx on aztec_v2.rollups_parsed (call_tx_hash);
CREATE INDEX IF NOT EXISTS aztec_v2_rollups_parsed_block_time_idx on aztec_v2.rollups_parsed (call_block_time);
CREATE INDEX IF NOT EXISTS aztec_v2_rollups_parsed_rollup_id_idx on aztec_v2.rollups_parsed (rollupid);