{{ config(
    schema = 'sudoswap_v2_ethereum',
    alias = 'base_trades',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_number','tx_hash','sub_tx_trade_id'],
    post_hook='{{ expose_spells(\'["ethereum"]\',
                            "project",
                            "sudoswap",
                            \'["ilemi"]\') }}'
    )
}}

{{sudoswap_v2_trades(
     blockchain = 'ethereum'
     ,platform_fee_address = '0xa020d57ab0448ef74115c112d18a9c231cc86000')
}}
