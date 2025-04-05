{{
    config(
        schema = 'ape_store',
        alias = 'trades',
        materialized = 'view',
        post_hook = '{{ expose_spells(
                        blockchains = \'["base", "ethereum"]\',
                        spell_type = "project",
                        spell_name = "ape_store",
                        contributors = \'["whale_hunter"]\') }}'
    )
}}

{% set ape_store_models = [
    ref('ape_store_base_trades')
    , ref('ape_store_ethereum_trades')
] %}

{% for model in ape_store_models %}
SELECT block_time,
       block_date,
       block_month,
       blockchain,
       platform,
       type,
       amount_usd,
       token_bought_amount,
       token_bought_symbol,
       token_bought_address,
       token_sold_amount,
       token_sold_symbol,
       token_sold_address,
       user,
       tx_hash,
       tx_index
FROM {{ model }}
{% if not loop.last %}
UNION ALL
{% endif %}
{% endfor %}