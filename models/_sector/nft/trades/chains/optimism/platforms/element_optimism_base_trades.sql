{{ config(
    schema = 'element_optimism',
    
    alias = 'base_trades',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_number','tx_hash','sub_tx_trade_id'],
    )
}}

WITH base_trades as (
    {{ element_v1_base_trades(
          blockchain = 'optimism'
        , erc721_sell_order_filled = source('element_ex_optimism','ElementEx_evt_ERC721SellOrderFilled')
        , erc721_buy_order_filled = source('element_ex_optimism','ElementEx_evt_ERC721BuyOrderFilled')
        )
    }}
)

SELECT * FROM base_trades
