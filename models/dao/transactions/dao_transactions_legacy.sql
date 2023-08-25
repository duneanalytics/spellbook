{{ config(
	tags=['legacy'],
    alias = alias('transactions', legacy_model=True),
    materialized = 'view',
    file_format = 'delta',
    post_hook='{{ expose_spells(\'["ethereum", "gnosis", "polygon"]\',
                                "sector",
                                "dao",
                                \'["Henrystats"]\') }}')
}}


{% set blockchains_models = [
ref('dao_transactions_ethereum_erc20_legacy')
,ref('dao_transactions_ethereum_eth_legacy')
,ref('dao_transactions_gnosis_erc20_legacy')
,ref('dao_transactions_gnosis_eth_legacy')
,ref('dao_transactions_polygon_erc20_legacy')
,ref('dao_transactions_polygon_eth_legacy')
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