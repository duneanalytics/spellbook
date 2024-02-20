{{ config(
    schema = 'social_avalanche_c',
    alias = 'base_trades',
    materialized = 'view'
    )
}}

{% set base_models = [
     ref('stars_arena_avalanche_c_base_trades')
] %}


WITH base_union AS (
    SELECT *
    FROM (
    {% for base_model in base_models %}
        SELECT
            blockchain
            , block_time
            , block_number
            , project
            , trader
            , subject
            , trade_side
            , amount_original
            , share_amount
            , subject_fee_amount
            , protocol_fee_amount
            , currency_contract
            , currency_symbol  --this field gets overriden in final social.trades spell
            , supply
            , tx_hash
            , evt_index
            , contract_address
        FROM {{ base_model }}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
        {% endfor %}
    )
)

--can be removed once decoded tables are fully denormalized
{{
    add_tx_columns(
        model_cte = 'base_union'
        , blockchain = 'avalanche_c'
        , columns = ['from', 'to']
    )
}}
