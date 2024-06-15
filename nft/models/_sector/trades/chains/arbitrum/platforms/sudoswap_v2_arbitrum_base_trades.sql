{{ config(
    schema = 'sudoswap_v2_arbitrum',
    alias = 'base_trades',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_number','tx_hash','sub_tx_trade_id'],
    post_hook='{{ expose_spells(\'["arbitrum"]\',
                            "project",
                            "sudoswap",
                            \'["ilemi"]\') }}'
    )
}}

WITH trades_final as (
{{sudoswap_v2_trades(
     blockchain = 'arbitrum'
     ,platform_fee_address = '0x6132912d8009268dcc457c003a621a0de405dbe0')
}})

{{ add_nft_tx_data('trades_final', 'arbitrum') }}
