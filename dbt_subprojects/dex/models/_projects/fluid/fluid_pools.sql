{{ config(
        schema = 'fluid',
        alias = 'pools',
        post_hook='{{ expose_spells(blockchains = \'["base","ethereum","polygon","arbitrum"]\',
                                      spell_type = "project", 
                                      spell_name = "fluid", 
                                      contributors = \'["Henrystats","dknugo"]\') }}'
        )
}}


{% set fluid_models = [
ref('fluid_v1_ethereum_pools')
, ref('fluid_arbitrum_pools')
, ref('fluid_base_pools')
, ref('fluid_polygon_pools')
] %}

with 

all_pools as (
    SELECT *
    FROM (
        {% for dex_pool_model in fluid_models %}
        SELECT
            blockchain
            , project
            , version
            , block_time 
            , block_number 
            , evt_index 
            , tx_hash 
            , factory 
            , dex 
            , supply_token 
            , borrow_token 
            , dex_id 
        FROM {{ dex_pool_model }}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
        {% endfor %}
    )
),

latest_fees as (
    select 
        dex 
        , blockchain 
        , max_by(fee, (block_number, evt_index)) as fee 
        , max_by(revenue_cut, (block_number, evt_index)) as revenue_cut 
    from (
    select 
        blockchain
        , dex 
        , block_number 
        , evt_index 
        , fee 
        , tx_hash 
        , revenue_cut 
    from 
    {{ ref('fluid_dex_initializations') }}

    union all 

    select 
        blockchain
        , dex 
        , block_number 
        , evt_index 
        , fee 
        , tx_hash 
        , revenue_cut 
    from 
    {{ ref('fluid_dex_fee_updates') }}
    ) x 
    group by 1, 2 
)

        select 
            ap.blockchain
            , ap.project
            , ap.version
            , ap.block_time as creation_block_time
            , ap.block_number as creation_block_number
            , ap.evt_index 
            , ap.tx_hash 
            , ap.factory 
            , ap.dex 
            , ap.supply_token 
            , ap.borrow_token 
            , ap.dex_id
            , sup.symbol as supply_token_symbol 
            , bor.symbol as borrow_token_symbol 
            , sup.decimals as supply_token_decimals
            , bor.decimals as borrow_token_decimals 
            , fdl.isSmartCol
            , fdl.isSmartDebt
            , lf.fee 
            , lf.revenue_cut 
        from 
        all_pools ap 
        left join 
        {{ source('tokens', 'erc20') }} sup 
            on ap.supply_token = sup.contract_address 
            and ap.blockchain = sup.blockchain 
        left join 
        {{ source('tokens', 'erc20') }} bor
            on ap.borrow_token = bor.contract_address 
            and ap.blockchain = bor.blockchain
        left join 
        {{ ref('fluid_dex_initializations') }} fdl 
            on ap.dex = fdl.dex 
            and ap.blockchain = fdl.blockchain
        left join 
        latest_fees lf 
            on ap.dex = lf.dex 
            and ap.blockchain = lf.blockchain