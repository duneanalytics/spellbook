{{ config(
        schema='prices',
        alias = 'tokens',
        materialized='table',
        file_format = 'delta',
        tags = ['static'],
        post_hook = '{{ hide_spells() }}'
        )
}}

{% set fungible_prices_models = [
    ref('prices_arbitrum_tokens')
    ,ref('prices_avalanche_c_tokens')
    ,ref('prices_bitcoin_tokens')
    ,ref('prices_bnb_tokens')
    ,ref('prices_cardano_tokens')
    ,ref('prices_ethereum_tokens')
    ,ref('prices_fantom_tokens')
    ,ref('prices_flare_tokens')
    ,ref('prices_flow_tokens')
    ,ref('prices_gnosis_tokens')
    ,ref('prices_hemi_tokens')
    ,ref('prices_optimism_tokens')
    ,ref('prices_polygon_tokens')
    ,ref('prices_solana_tokens')
    ,ref('prices_celo_tokens')
    ,ref('prices_base_tokens')
    ,ref('prices_zksync_tokens')
    ,ref('prices_zora_tokens')
    ,ref('prices_scroll_tokens')
    ,ref('prices_linea_tokens')
    ,ref('prices_zkevm_tokens')
    ,ref('prices_mantle_tokens')
    ,ref('prices_blast_tokens')
    ,ref('prices_sei_tokens')
    ,ref('prices_nova_tokens')
    ,ref('prices_worldchain_tokens')
    ,ref('prices_kaia_tokens')
    ,ref('prices_tron_tokens')
    ,ref('prices_ronin_tokens')
    ,ref('prices_boba_tokens')
    ,ref('prices_viction_tokens')
    ,ref('prices_corn_tokens')
    ,ref('prices_sonic_tokens')
    ,ref('prices_ink_tokens')
    ,ref('prices_sophon_tokens')
    ,ref('prices_tac_tokens')
    ,ref('prices_opbnb_tokens')
    ,ref('prices_taiko_tokens')
    ,ref('prices_unichain_tokens')
    ,ref('prices_abstract_tokens')
    ,ref('prices_berachain_tokens')
    ,ref('prices_apechain_tokens')
    ,ref('prices_shape_tokens')
    ,ref('prices_degen_tokens')
    ,ref('prices_lens_tokens')
    ,ref('prices_plume_tokens')
    ,ref('prices_katana_tokens')
    ,ref('prices_superseed_tokens')
    ,ref('prices_sui_tokens')
    ,ref('prices_hyperevm_tokens')
    ,ref('prices_peaq_tokens')
    ,ref('prices_somnia_tokens')
    ,ref('prices_plasma_tokens')
] %}

with fungible_tokens as (
    select *
    from
    (
        {% for model in fungible_prices_models -%}
        select
            token_id
            , blockchain
            , contract_address
            , symbol
            , decimals
        from {{ model }}
        where contract_address is distinct from 0x0000000000000000000000000000000000000000
        {% if not loop.last -%}
        union all
        {% endif -%}
        {% endfor -%}
    )
)
, fungible as (
    select
        p.token_id
        ,p.blockchain
        ,p.contract_address
        ,coalesce(erc20.symbol, p.symbol) as symbol
        ,coalesce(erc20.decimals, p.decimals) as decimals
    from
        fungible_tokens as p
    left join {{source('tokens','erc20')}} as erc20
        on p.blockchain = erc20.blockchain
        and p.contract_address = erc20.contract_address
    where
        p.blockchain is not null
        and p.contract_address is not null
)
select *
from fungible
union all
select *
from {{ref('prices_native_tokens')}}