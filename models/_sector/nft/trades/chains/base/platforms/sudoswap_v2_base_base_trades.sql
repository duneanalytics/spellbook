{{ config(
    schema = 'sudoswap_v2_base',
    alias = 'base_trades',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_number','tx_hash','sub_tx_trade_id'],
    post_hook='{{ expose_spells(\'["base"]\',
                            "project",
                            "sudoswap",
                            \'["ilemi"]\') }}'
    )
}}

WITH trades_final as (
{{sudoswap_v2_trades(
     blockchain = 'base'
     ,platform_fee_address = '0x8ce608ce2b5004397faef1556bfe33bdfbe4696d')
}})

{{ add_nft_tx_data('trades_final', 'base') }}
