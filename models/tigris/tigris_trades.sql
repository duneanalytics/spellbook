{{ config(
    
        alias = 'trades',
        post_hook='{{ expose_spells(\'["polygon","arbitrum"]\',
                                "project",
                                "tigris",
                                \'["Henrystats"]\') }}'
        )
}}

{% set models = [
ref('tigris_polygon_trades')
,ref('tigris_arbitrum_trades')
] %}


SELECT *
FROM (
    {% for model in models %}
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
        positions_contract,
        protocol_version
    FROM {{ model }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)

-- reload
