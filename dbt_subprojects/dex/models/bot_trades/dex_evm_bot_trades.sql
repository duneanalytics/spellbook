{{
    config(
        schema = 'dex_evm',
        alias = 'bot_trades',
        materialized = 'view',
        post_hook = '{{ expose_spells(
                        blockchains = \'["ethereum", "base", "blast", "arbitrum", "bnb", "avalanche_c"]\',
                        spell_type = "sector",
                        spell_name = "bot_trades",
                        contributors = \'["whale_hunter"]\') }}'
    )
}}



{% set evm_trading_bots = [
    ref('banana_gun_ethereum_bot_trades')
    ,ref('banana_gun_base_bot_trades')
    ,ref('pepeboost_ethereum_bot_trades')
    ,ref('flokibot_ethereum_bot_trades')
    ,ref('flokibot_base_bot_trades')
] %}

{% for bot in evm_trading_bots %}
SELECT block_time,
       block_date,
       block_month,
       bot,
       block_number,
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
       fee_percentage_fraction,
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
FROM {{ bot }}
{% if not loop.last %}
UNION ALL
{% endif %}
{% endfor %}