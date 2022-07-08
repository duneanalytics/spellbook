-- this uses the contract aliases to identify and categorize erc20 transactions that involve Aztec Connect 

-- filter txns down to only relevant txns to prevent double counting
create or replace view aztec_v2.view_rollup_bridge_transfers as 
with tfers_raw as (
  select distinct t.*
    from erc20."ERC20_evt_Transfer" t
    inner join aztec_v2.contract_labels c 
      on t."from" = c.contract_address
      or t."to" = c.contract_address
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
select * from tfers_categorized