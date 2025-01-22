{{
    config(
        schema = 'immortalx',
        alias = 'perpetual_trades',
        post_hook = '{{ expose_spells(\'["celo"]\',
                                        "project",
                                        "immortalx",
                                        \'["tomfutago"]\') }}'
    )
}}

{% set immortalx_perpetual_trade_models = [
    ref('immortalx_celo_perpetual_trades')
] %}

{% for immortalx_perpetual_model in immortalx_perpetual_trade_models %}
SELECT
    blockchain,
    block_date,
    block_month,
    block_time,
    virtual_asset,
    underlying_asset,
    market,
    market_address,
    volume_usd,
    fee_usd,
    margin_usd,
    trade,
    project,
    version,
    frontend,
    trader,
    volume_raw,
    tx_hash,
    tx_from,
    tx_to,
    evt_index
FROM {{ immortalx_perpetual_model }}
{% if not loop.last %}
UNION ALL
{% endif %}
{% endfor %}
