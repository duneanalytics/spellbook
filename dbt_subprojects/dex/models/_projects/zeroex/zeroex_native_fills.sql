{{ config(
        schema = 'zeroex',
        alias = 'native_fills',
        post_hook='{{ expose_spells(\'["ethereum","arbitrum", "optimism", "polygon","bnb"]\',
                                "project",
                                "zeroex",
                                \'["rantum","bakabhai993"]\') }}'
        )
}}

{% set zeroex_models = [  
ref('zeroex_ethereum_native_fills')
,ref('zeroex_polygon_native_fills')
,ref('zeroex_optimism_native_fills')
,ref('zeroex_arbitrum_native_fills')
,ref('zeroex_bnb_native_fills')
] %}


SELECT *
FROM (
    {% for model in zeroex_models %}
    SELECT
        block_time,
        block_number,
        version,
        block_date,
        block_month,
        tx_hash,
        evt_index,
        maker,
        taker,
        maker_token,
        maker_token_amount_raw,
        taker_token_amount_raw,
        maker_symbol,
        token_pair,
        maker_token_amount,
        taker_token,
        taker_symbol,
        taker_token_amount,
        matcha_limit_order_flag,
        volume_usd,
        protocol_fee_paid_eth,
        blockchain,
        contract_address,
        native_order_type,
        tx_from,
        tx_to
    FROM {{ model }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)
