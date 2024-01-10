{{ config(
    
    alias = 'transactions',
    materialized = 'view',
    post_hook='{{ expose_spells(\'["ethereum", "gnosis", "polygon", "base", "arbitrum"]\',
                                "sector",
                                "dao",
                                \'["Henrystats"]\') }}')
}}


{% set blockchains_models = [
ref('dao_transactions_ethereum_erc20')
,ref('dao_transactions_ethereum_eth')
,ref('dao_transactions_gnosis_erc20')
,ref('dao_transactions_gnosis_eth')
,ref('dao_transactions_polygon_erc20')
,ref('dao_transactions_polygon_eth')
,ref('dao_transactions_base_erc20')
,ref('dao_transactions_base_eth')
,ref('dao_transactions_arbitrum_erc20')
,ref('dao_transactions_arbitrum_eth')
] %}

SELECT *

FROM (
    {% for transactions_model in blockchains_models %}
    SELECT
        blockchain,
        dao_creator_tool,
        dao,
        dao_wallet_address,
        block_date,
        block_time,
        tx_type,
        asset_contract_address,
        asset,
        raw_value,
        value,
        usd_value,
        tx_hash,
        tx_index,
        address_interacted_with,
        trace_address
    FROM {{ transactions_model }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)