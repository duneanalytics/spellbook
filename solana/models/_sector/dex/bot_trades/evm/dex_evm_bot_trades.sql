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



{% set evm_trading_bot = [
    ref('banana_gun_evm_bot_trades')
] %}

{% for bot in solana_trading_bot %}
SELECT block_time,
       block_date,
       block_month,
       blockchain,
       amount_usd,
       type,
       token_bought_amount,
       token_bought_symbol,
       token_bought_address,
       token_sold_amount,
       token_sold_symbol,
       token_sold_address,
       fee_usd,
       fee_token_amount,
       fee_token_symbol,
       fee_token_address,
       project,
       version,
       token_pair,
       project_contract_address,
       user,
       -- TODO
       tx_id,
       tx_index,
       outer_instruction_index,
       inner_instruction_index,
       is_last_trade_in_transaction
FROM {{ bot }}
{% if not loop.last %}
UNION ALL
{% endif %}
{% endfor %}