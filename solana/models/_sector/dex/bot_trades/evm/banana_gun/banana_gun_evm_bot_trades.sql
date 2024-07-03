{{
    config(
        schema = 'bot_trades',
        alias = 'banana_gun_evm',
        materialized = 'view',
        post_hook = '{{ expose_spells(
                        blockchains = \'["ethereum", "base", "avalanche_c", "blast", "bnb"]\',
                        spell_type = "sector",
                        spell_name = "banana_gun",
                        contributors = \'["whale_hunter"]\') }}'
    )
}}



   {# ref('banana_gun_avalanche_c_bot_trades')
    , ref('banana_gun_base_bot_trades')
    , ref('banana_gun_blast_bot_trades')
    , ref('banana_gun_bnb_bot_trades')
    , #}
{% set evm_trading_bot = [
   ref('banana_gun_ethereum_bot_trades')
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