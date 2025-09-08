{{ config(
        schema = 'fluid',
        alias = 'pools'
        )
}}

{% set fluid_models = [
ref('fluid_v1_ethereum_pools')
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
)

        select 
            ap.blockchain
            , project
            , version
            , block_time as creation_block_time
            , block_number as creation_block_number
            , evt_index 
            , tx_hash 
            , factory 
            , dex 
            , supply_token 
            , borrow_token 
            , dex_id
            , sup.symbol as supply_token_symbol 
            , bor.symbol as borrow_token_symbol 
            , sup.decimals as supply_token_decimals
            , bor.decimals as borrow_token_decimals 
        from 
        all_pools ap 
        left join 
        {{ source('tokens', 'erc20') }} sup 
            on ap.supply_token = sup.contract_address 
            and ap.blockchain = sup.blockchain 
        left join 
        {{ source('tokens', 'erc20') }} bor
            on ap.supply_token = bor.contract_address 
            and ap.blockchain = bor.blockchain 
