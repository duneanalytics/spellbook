{{
    config(
        schema = 'account_abstraction_erc4337_celo',
        alias = 'v0_6_userops_basics',
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['userop_hash', 'tx_hash']
    )
}}

-- macros/models/sector/erc4337
-- dedup: a single bundle tx can contain handleOps calls on more than one EntryPoint
-- (each with its own beneficiary), so the tx_hash join in the macro can fan out a userop.
-- Keep one row per unique_key so the incremental MERGE source never has duplicate keys.
with base as (
    {{
        erc4337_userops_basics(
            blockchain = 'celo',
            version = 'v0.6',
            userops_evt_model = source('erc4337_celo', 'EntryPoint_evt_UserOperationEvent'),
            handleops_call_model = source('erc4337_celo', 'EntryPoint_call_handleOps')
        )
    }}
)
, deduped as (
    select
          blockchain
        , version
        , block_month
        , block_time
        , entrypoint_contract
        , tx_hash
        , sender
        , userop_hash
        , success
        , paymaster
        , op_fee
        , beneficiary
        , row_number() over (
            partition by userop_hash, tx_hash
            order by (beneficiary is not null) desc, beneficiary desc
        ) as _row_num
    from base
)
select
      blockchain
    , version
    , block_month
    , block_time
    , entrypoint_contract
    , tx_hash
    , sender
    , userop_hash
    , success
    , paymaster
    , op_fee
    , beneficiary
from deduped
where _row_num = 1
