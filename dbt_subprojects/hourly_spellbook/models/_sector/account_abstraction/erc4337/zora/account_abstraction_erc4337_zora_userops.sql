{{ config(
    alias = 'userops',
    schema = 'account_abstraction_erc4337_zora',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['userop_hash', 'tx_hash'],
    post_hook='{{ expose_spells(\'["zora"]\',
                                    "project",
                                    "erc4337",
                                    \'["0xbitfly", "hosuke", "peterrliem","intensodefi"]\') }}'
)}}

-- deployed_date
{% set chain = 'zora' %}
{% set gas_symbol = 'ETH' %}
{% set wrapped_gas_address = '0x4200000000000000000000000000000000000006' %}
{% set deployed_date = '2023-06-20' %}

{% set erc4337_models = [
    ref('account_abstraction_erc4337_zora_v0_6_userops_basics')
    , ref('account_abstraction_erc4337_zora_v0_7_userops_basics')
] %}

with userop as(
    SELECT *
    FROM (
        {% for erc4337_model in erc4337_models %}
        SELECT
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
        FROM {{ erc4337_model }}
        {% if is_incremental() %}
        WHERE {{ incremental_predicate('block_time') }}
        {% endif %}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
        {% endfor %}
    )
)
, txs as (
    select
          hash as tx_hash
        , tx."from" as tx_from
        , tx.to as tx_to
        , '{{gas_symbol}}' as gas_symbol
        , ((cast(gas_used as double) * gas_price)+l1_fee) / 1e18 as tx_fee
    from {{ source('zora', 'transactions') }} tx
    where hash in (
        select tx_hash from userop
    )
    and block_time > date '{{deployed_date}}'
    {% if is_incremental() %}
        and {{ incremental_predicate('block_time') }}
    {% endif %}
)
, price as (
    select symbol, decimals, minute, price
    from {{source('prices','usd')}}
    where minute > date '{{deployed_date}}'
        and contract_address={{wrapped_gas_address}}
        and blockchain='zora'
        {% if is_incremental() %}
         and minute >= date_trunc('day', now() - interval '7' day)
        {% endif %}
)
select
      userop.blockchain
    , userop.version
    , userop.block_month
    , userop.block_time
    , userop.entrypoint_contract
    , userop.tx_hash
    , userop.sender
    , userop.userop_hash
    , userop.success
    , userop.paymaster
    , userop.op_fee as op_fee
    , userop.op_fee * price.price as op_fee_usd
    , txs.tx_from as bundler
    , txs.tx_to
    , txs.gas_symbol
    , txs.tx_fee
    , txs.tx_fee * price.price as tx_fee_usd
    , userop.beneficiary
from userop
left join txs on userop.tx_hash = txs.tx_hash
left join price on date_trunc('minute', userop.block_time) = price.minute