{{ config(
    schema='prices',
    alias = 'tokens_legacy',
    materialized='table',
    file_format = 'delta',
    tags = ['static']
)
}}

/* only add here chains which will exist in EVM dex.trades */
{% set evm_models = [
    ref('prices_abstract_tokens')
    ,ref('prices_apechain_tokens')
    ,ref('prices_arbitrum_tokens')
    ,ref('prices_avalanche_c_tokens')
    ,ref('prices_base_tokens')
    ,ref('prices_berachain_tokens')
    ,ref('prices_bnb_tokens')
    ,ref('prices_blast_tokens')
    ,ref('prices_boba_tokens')
    ,ref('prices_celo_tokens')
    ,ref('prices_corn_tokens')
    ,ref('prices_ethereum_tokens')
    ,ref('prices_fantom_tokens')
    ,ref('prices_flare_tokens')
    ,ref('prices_gnosis_tokens')
    ,ref('prices_hemi_tokens')
    ,ref('prices_ink_tokens')
    ,ref('prices_katana_tokens')
    ,ref('prices_kaia_tokens')
    ,ref('prices_linea_tokens')
    ,ref('prices_mantle_tokens')
    ,ref('prices_nova_tokens')
    ,ref('prices_opbnb_tokens')
    ,ref('prices_optimism_tokens')
    ,ref('prices_plume_tokens')
    ,ref('prices_polygon_tokens')
    ,ref('prices_ronin_tokens')
    ,ref('prices_scroll_tokens')
    ,ref('prices_sei_tokens')
    ,ref('prices_shape_tokens')
    ,ref('prices_sonic_tokens')
    ,ref('prices_sophon_tokens')
    ,ref('prices_superseed_tokens')
    ,ref('prices_tac_tokens')
    ,ref('prices_taiko_tokens')
    ,ref('prices_unichain_tokens')
    ,ref('prices_viction_tokens')
    ,ref('prices_worldchain_tokens')
    ,ref('prices_zkevm_tokens')
    ,ref('prices_zksync_tokens')
    ,ref('prices_zora_tokens')
] %}

/* only add here chains which will NOT exist in EVM dex.trades */
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
        , coalesce(db.token_symbol, n.symbol) as symbol
        , db.token_decimals as decimals
    from
        {{ ref('prices_native_tokens') }} as n
    left join
        {{ source('dune', 'blockchains') }} as db
        on n.symbol = db.token_symbol
)
, fungible_non_evm as (
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