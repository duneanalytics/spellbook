{{ config(
        schema = 'gas',
        tags = 'prod_exclude',
        alias = 'fees',
        post_hook='{{ expose_spells(\'["arbitrum","avalanche_c","base","bnb","ethereum","optimism","scroll","zksync","zora"]\',
                                "sector",
                                "gas",
                                \'["soispoke", "ilemi", "0xRob", "jeff-dude"]\') }}'
        )
}}

{% set gas_fees_models = [
    'gas_arbitrum_fees'
    ,'gas_avalanche_c_fees'
    ,'gas_base_fees'
    ,'gas_bnb_fees'
    ,'gas_ethereum_fees'
    ,'gas_optimism_fees'
    ,'gas_scroll_fees'
    ,'gas_zksync_fees'
    ,'gas_zora_fees'
] %}

--remove mantle for now:
    --'gas_mantle_fees'

SELECT
    *
FROM
(
    {% for gas_model in gas_fees_models %}
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
        {{ ref(gas_model) }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)
