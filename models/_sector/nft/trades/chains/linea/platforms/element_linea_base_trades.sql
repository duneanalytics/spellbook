{{ config(
    schema = 'element_linea',
    
    alias = 'base_trades',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_number','tx_hash','sub_tx_trade_id'],
    )
}}

WITH base_trades as (
    {{ element_v1_base_trades(
          blockchain = 'linea'
        , erc721_sell_order_filled = NULL --source('element_ex_linea','ElementEx_evt_ERC721SellOrderFilled')
        , erc721_buy_order_filled = NULL --source('element_ex_linea','ElementEx_evt_ERC721BuyOrderFilled')
        , erc1155_sell_order_filled = source('element_ex_linea','ElementEx_evt_ERC1155SellOrderFilled')
        , erc1155_buy_order_filled = source('element_ex_linea','ElementEx_evt_ERC1155BuyOrderFilled')
        )
    }}
)

-- this will be removed once tx_from and tx_to are available in the base event tables
{{ add_nft_tx_data('base_trades', 'linea') }}