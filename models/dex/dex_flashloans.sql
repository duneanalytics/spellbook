{{ config(
        tags=['dunesql'],
        alias = alias('flashloans'),
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['blockchain', 'tx_hash', 'evt_index'],
        post_hook='{{ expose_spells(\'["ethereum", "bnb", "avalanche_c", "optimism", "arbitrum", "fantom", "polygon"]\',
                                "sector",
                                "dex",
                                \'["hildobby"]\') }}'
        )
}}

{% set flashloan_models = [
ref('aave_flashloans')
, ref('balancer_flashloans')
, ref('dydx_ethereum_flashloans')
, ref('uniswap_flashloans')
, ref('euler_ethereum_flashloans')
, ref('maker_ethereum_flashloans')
, ref('synapse_flashloans')
, ref('fiat_dao_ethereum_flashloans')
, ref('equalizer_flashloans')
] %}


SELECT *
FROM (
    {% for flash_model in flashloan_models %}
    SELECT
        blockchain,
        project,
        version,
        block_month,
        block_time,
        amount,
        amount_usd,
        tx_hash,
        evt_index,
        fee,
        currency_contract,
        currency_symbol,
        contract_address
    FROM {{ flash_model }}
    {% if not loop.last %}
    {% if is_incremental() %}
    WHERE block_time >= date_trunc('day', now() - interval '7' Day)
    {% endif %}
    UNION ALL
    {% endif %}
    {% endfor %}
)
