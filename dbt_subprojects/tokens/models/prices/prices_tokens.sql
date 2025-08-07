{{ config(
        schema='prices',
        alias = 'tokens',
        materialized='table',
        file_format = 'delta',
        tags = ['static'],
        post_hook = '{{ hide_spells() }}'
        )
}}

{% set evm_models = [
    ref('prices_coinpaprika_trusted_tokens')
] %}

{% set non_evm_models = [
    ref('prices_bitcoin_tokens')
    ,ref('prices_cardano_tokens')
    ,ref('prices_degen_tokens')
    ,ref('prices_lens_tokens')
    ,ref('prices_solana_tokens')
    ,ref('prices_tron_tokens')
] %}

with native as (
    -- handle native separately due to unique use case with null blockchain/address
    select
        n.token_id
        , db.name as blockchain
        , db.token_address as contract_address
        , db.token_symbol as symbol
        , db.token_decimals as decimals
    from
        {{ ref('prices_native_tokens') }} as n
    inner join
        {{ source('dune', 'blockchains') }} as db
        on n.symbol = db.token_symbol
)
, fungible_non_evm as (
    -- source all non-EVM chains excluded from prices pipeline
    {% for model in non_evm_models -%}
    select
        pt.token_id
        , pt.blockchain
        , pt.contract_address
        , pt.symbol
        , pt.decimals
    from
        {{ model }} as pt
    left join
        {{ source('dune', 'blockchains') }} as anti
        on anti.name = pt.blockchain
        and anti.token_address = pt.contract_address
    where
        anti.name is null -- ignore any potential native manual entries
    {% if not loop.last -%}
    union all
    {% endif -%}
    {% endfor -%}
)
, fungible_evm as (
    -- only source trusted tokens for EVM chains in prices pipeline
    {% for model in evm_models -%}
    select
        pt.token_id
        , pt.blockchain
        , pt.contract_address
        , pt.symbol
        , pt.decimals
    from
        {{ model }} as pt
    left join
        {{ source('dune', 'blockchains') }} as anti
        on anti.name = pt.blockchain
        and anti.token_address = pt.contract_address
    where
        anti.name is null -- ignore any potential native manual entries
    {% if not loop.last -%}
    union all
    {% endif -%}
    {% endfor -%}
)
, final as (
    select * from native
    union all
    select * from fungible_non_evm
    union all
    select * from fungible_evm
)
select
    *
from
    final