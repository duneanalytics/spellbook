{{ config(
        schema = 'gas',
        alias = 'fees',
        post_hook='{{ expose_spells(\'["arbitrum","avalanche_c","base","bnb","ethereum","optimism","scroll","zksync","zora"]\',
                                "sector",
                                "gas",
                                \'["soispoke", "ilemi", "0xRob", "jeff-dude"]\') }}'
        )
}}

{% set chains = [
    "arbitrum", "avalanche_c", "base", "blast", "bnb", "celo", "ethereum",
    "fantom", "gnosis", "linea", "mantle", "optimism", "polygon", "scroll",
    "sei", "zkevm", "zksync", "zora"
] %}


SELECT
    *
FROM
(
    {% for blockchain in chains %}
    SELECT
        blockchain
        ,block_month
        ,block_date
        ,block_time
        ,block_number
        ,tx_hash
        ,tx_sender
        ,tx_receiver
        ,native_token_symbol
        ,tx_amount_native
        ,tx_amount_usd
        ,tx_fee_native
        ,tx_fee_usd
        ,burned_native
        ,burned_usd
        ,validator
        ,gas_price_gwei
        ,gas_price_usd
        ,gas_used
        ,gas_limit
        ,gas_usage_percent
        ,transaction_type
    FROM
        {{ ref('gas_' ~ blockchain ~ '_fees') }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)
