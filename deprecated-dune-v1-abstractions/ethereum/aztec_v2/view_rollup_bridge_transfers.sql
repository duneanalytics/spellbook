-- this uses the contract aliases to identify and categorize erc20 and ETH transactions that involve Aztec Connect 

-- filter txns down to only relevant txns to prevent double counting
create or replace view aztec_v2.view_rollup_bridge_transfers as 
with erc_tfers_filtered as (
  -- get the erc20 tokens
  select distinct t.*
    from erc20."ERC20_evt_Transfer" t
    inner join aztec_v2.contract_labels c 
      on t."from" = c.contract_address
      or t."to" = c.contract_address
)
, eth_traces_filtered as (
    select distinct t.*
    from ethereum."traces" t
    inner join aztec_v2.contract_labels c 
          on t."from" = c.contract_address
          or t."to" = c.contract_address
)
, tfers_raw as (
    select *
    from erc_tfers_filtered
  union all 
  -- Track the ETH that's transferred
  SELECT "from"
      , "to"
      , value
      , '\xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee'::bytea as contract_address
      , tx_hash as evt_tx_hash
      , null::bigint as evt_index
      , block_time as evt_block_time
      , block_number as evt_block_number
  FROM eth_traces_filtered
  WHERE true
    and value <> 0
    AND (LOWER(call_type) NOT IN ('delegatecall', 'callcode', 'staticcall') or call_type is null)
    AND CASE WHEN block_number < 4370000  THEN True
            WHEN block_number >= 4370000 THEN tx_success
            END 
    AND success = true
  -- subtract the gas that was used
  -- This only really happens when the account is user-controlled, so it doesn't actually
  -- apply to any of the contracts in aztec_v2.contract_labels
  /*
  union all
  SELECT "from"
      , '\x0000000000000000000000000000000000000000'::bytea as "to"
      , gas_price*"gas_used" as value
      , '\xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee'::bytea as contract_address
      , hash as evt_tx_hash
      , null as evt_index
      , block_time as evt_block_time
      , block_number as evt_block_number
  FROM ethereum."transactions" t
  inner join aztec_v2.contract_labels c 
      on t."from" = c.contract_address
    */
)
, tfers_categorized as (
  select t.*
    , tk.symbol
    , tk.decimals
    , t.value / 10^(coalesce(tk.decimals,18)) as value_norm
    , case when to_contract.contract_type is not null and from_contract.contract_type is not null then 'Internal'
      else 'External'        
        end as broad_txn_type
    , case 
        -- You can think of "burned" and "minted" as bridge-protocol interactions, actually
        -- when "to" = '\x0000000000000000000000000000000000000000'::bytea then 'Burned'
        -- when "from" = '\x0000000000000000000000000000000000000000'::bytea then 'Minted'
        when from_contract.contract_type is null and to_contract.contract_type = 'Rollup' then 'User Deposit'
        when to_contract.contract_type is null and from_contract.contract_type = 'Rollup' then 'User Withdrawal'
        when from_contract.contract_type = 'Rollup' and to_contract.contract_type = 'Bridge' then 'RP to Bridge'
        when to_contract.contract_type = 'Rollup' and from_contract.contract_type = 'Bridge' then 'Bridge to RP'
        when from_contract.contract_type = 'Bridge' and to_contract.contract_type is null then 'Bridge to Protocol'
        when to_contract.contract_type = 'Bridge' and from_contract.contract_type is null then 'Protocol to Bridge'
        end as spec_txn_type
    , to_contract.protocol as to_protocol
    , to_contract.contract_type as to_type
    , from_contract.protocol as from_protocol
    , from_contract.contract_type as from_type
    , case when to_contract.contract_type = 'Bridge' then to_contract.contract_address
      when from_contract.contract_type = 'Bridge' then from_contract.contract_address
      else null end
      as bridge_address
    , case when to_contract.contract_type = 'Bridge' then to_contract.protocol
      when from_contract.contract_type = 'Bridge' then from_contract.protocol
      else null end
      as bridge_protocol
    , case when to_contract.contract_type = 'Bridge' then to_contract.version
      when from_contract.contract_type = 'Bridge' then from_contract.version
      else null end
      as bridge_version
  from tfers_raw t
  left join erc20.tokens tk on t.contract_address = tk.contract_address
  left join aztec_v2.contract_labels to_contract on t."to" = to_contract.contract_address
  left join aztec_v2.contract_labels from_contract on t."from" = from_contract.contract_address
)
select * from tfers_categorized;