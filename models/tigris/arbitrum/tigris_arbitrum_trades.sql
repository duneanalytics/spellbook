{{ config(
    tags=['dunesql'],
        alias = alias('trades')
        )
}}

{% set tigris_models = [
ref('tigris_v1_arbitrum_trades')
,ref('tigris_v2_arbitrum_trades')
] %}


SELECT *
FROM (
    {% for perpetual_model in tigris_models %}
    SELECT
        blockchain,
        block_month,
        day,
        project_contract_address,
        evt_block_time,
        evt_index,
        evt_tx_hash,
        position_id,
        price,
        new_margin,
        leverage,       
        volume_usd,
        margin_asset,
        pair,
        direction,
        referral,
        trader,
        margin_change,
        trade_type,
        version,
        positions_contract
    FROM {{ perpetual_model }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)