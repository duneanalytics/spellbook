{{ config(
        tags = ['dunesql'],
        alias = alias('trades')
        )
}}

{% set oneinch_models = [
ref('oneinch_ethereum_trades_v1')
,ref('oneinch_ethereum_trades_v2')
,ref('oneinch_ethereum_trades_v3')
,ref('oneinch_ethereum_trades_v4')
,ref('oneinch_ethereum_trades_v5')
,ref('oneinch_ethereum_trades_onesplit')
,ref('oneinch_ethereum_trades_oneproto')
,ref('oneinch_ethereum_trades_zeroex')
,ref('oneinch_ethereum_trades_unoswap_v3')
,ref('oneinch_ethereum_trades_unoswap_v4')
,ref('oneinch_ethereum_trades_unoswap_v5')
,ref('oneinch_ethereum_trades_uniswap_v3')
,ref('oneinch_ethereum_trades_clipper')
,ref('oneinch_ethereum_trades_limit_order_protocol_v1')
,ref('oneinch_ethereum_trades_limit_order_protocol_v2')
,ref('oneinch_ethereum_trades_limit_order_protocol_v3')
,ref('oneinch_ethereum_trades_limit_order_protocol_embedded_rfq')
,ref('oneinch_ethereum_trades_limit_order_protocol_rfq_v1')
,ref('oneinch_ethereum_trades_limit_order_protocol_rfq_v2')
,ref('oneinch_ethereum_trades_limit_order_protocol_rfq_v3')
] %}


SELECT *
FROM (
    {% for dex_model in oneinch_models %}
    SELECT
        blockchain
        ,project
        ,version
        ,block_date
        ,block_month
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
