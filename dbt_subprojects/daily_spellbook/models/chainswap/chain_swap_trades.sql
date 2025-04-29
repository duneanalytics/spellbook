{{
    config(
        schema = 'trades',
        alias = 'chain_swap',
        materialized = 'view',
        post_hook = '{{ expose_spells(
                        blockchains = \'["ethereum", "base", "avalanche_c", "optimism","polygon", "bnb", "arbitrum"]\',
                        spell_type = "project",
                        spell_name = "chain_swap",
                        contributors = \'["whale_hunter","clizzard"]\') }}'
    )
}}



{% set blockchains = [
 ref('chain_swap_ethereum_trades')
 , ref('chain_swap_base_trades')
 , ref('chain_swap_arbitrum_trades')
 , ref('chain_swap_optimism_trades')
 , ref('chain_swap_polygon_trades')
 , ref('chain_swap_bnb_trades')
 , ref('chain_swap_avalanche_c_trades')
] %}

{% for blockchain in blockchains %}
SELECT block_time,
       block_date,
       block_month,
       blockchain,
       -- Trade
       amount_usd,
       type,
       token_bought_amount,
       token_bought_symbol,
       token_bought_address,
       token_sold_amount,
       token_sold_symbol,
       token_sold_address,
       -- Fees
       fee_usd,
       fee_token_amount,
       fee_token_symbol,
       fee_token_address,
       -- Dex
       project,
       version,
       token_pair,
       project_contract_address,
       -- User
       user,
       tx_hash,
       evt_index,
       is_last_trade_in_transaction
FROM {{ blockchain }}
{% if not loop.last %}
UNION ALL
{% endif %}
{% endfor %}