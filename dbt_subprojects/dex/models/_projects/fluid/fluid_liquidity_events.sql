{{ config(
        schema = 'fluid',
        alias = 'liquidity_events'
        )
}}

{% set fluid_models = [
ref('fluid_arbitrum_liquidity_events')
, ref('fluid_ethereum_liquidity_events')
] %}


SELECT *
FROM (
    {% for dex_pool_model in fluid_models %}
    SELECT
        blockchain
        , version 
        , project 
        , block_time
        , tx_hash
        , evt_index
        , user_address
        , token_address
        , supply_amount
        , borrow_amount
        , supply_amount / (supply_exchange_price / 1e12) as supply_amount_raw
        , borrow_amount / (borrow_exchange_price / 1e12) as borrow_amount_raw
        , withdraw_to
        , borrow_to
        , total_amounts
        , exchange_prices_and_config
        , borrow_rate
        , supply_rate
        , fee
        , utilization
        , supply_exchange_price
        , borrow_exchange_price
        , supply_interest_raw
        , borrow_interest_raw
        , total_supply_interest_free
        , total_borrow_interest_free
        , total_supply_with_interest
        , total_borrow_with_interest
    FROM {{ dex_pool_model }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)