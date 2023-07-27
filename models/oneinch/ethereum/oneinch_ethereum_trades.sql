{{ config(
	tags=['legacy'],
	
        alias = alias('trades', legacy_model=True)
        )
}}

{% set oneinch_models = [
ref('oneinch_v1_ethereum_trades_legacy')
,ref('oneinch_v2_ethereum_trades_legacy')
,ref('oneinch_v3_ethereum_trades_legacy')
,ref('oneinch_v4_ethereum_trades_legacy')
,ref('oneinch_v5_ethereum_trades_legacy')
,ref('oneinch_onesplit_ethereum_trades_legacy')
,ref('oneinch_oneproto_ethereum_trades_legacy')
,ref('oneinch_zeroex_ethereum_trades_legacy')
,ref('oneinch_unoswap_v3_ethereum_trades_legacy')
,ref('oneinch_unoswap_v4_ethereum_trades_legacy')
,ref('oneinch_unoswap_v5_ethereum_trades_legacy')
,ref('oneinch_uniswap_v3_ethereum_trades_legacy')
,ref('oneinch_clipper_ethereum_trades_legacy')
,ref('oneinch_limit_order_protocol_v1_ethereum_trades_legacy')
,ref('oneinch_limit_order_protocol_v2_ethereum_trades_legacy')
,ref('oneinch_limit_order_protocol_v3_ethereum_trades_legacy')
,ref('oneinch_limit_order_protocol_embedded_rfq_ethereum_trades_legacy')
,ref('oneinch_limit_order_protocol_rfq_v1_ethereum_trades_legacy')
,ref('oneinch_limit_order_protocol_rfq_v2_ethereum_trades_legacy')
,ref('oneinch_limit_order_protocol_rfq_v3_ethereum_trades_legacy')
] %}


SELECT *
FROM (
    {% for dex_model in oneinch_models %}
    SELECT
        blockchain
        ,project
        ,version
        ,block_date
        ,block_time
        ,token_bought_symbol
        ,token_sold_symbol
        ,token_pair
        ,token_bought_amount
        ,token_sold_amount
        ,token_bought_amount_raw
        ,token_sold_amount_raw
        ,amount_usd
        ,token_bought_address
        ,token_sold_address
        ,taker
        ,maker
        ,project_contract_address
        ,tx_hash
        ,tx_from
        ,tx_to
        ,trace_address
        ,evt_index
    FROM {{ dex_model }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)
;