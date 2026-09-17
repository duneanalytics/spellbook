{% set blockchain = 'arc' %}

{{ config(
    schema = 'gas_' + blockchain
    ,alias = 'fees'
    ,partition_by = ['block_month']
    ,materialized = 'incremental'
    ,file_format = 'delta'
    ,incremental_strategy='merge'
    ,unique_key = ['block_month', 'tx_hash']
    ,incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

-- Arc's gas token is USDC, not ETH: the native asset sits at 0x00..00 with 18 decimals and
-- prices at ~$1. Nothing below hardcodes ETH -- evm_l1_gas_fees() reads the symbol, decimals,
-- contract address and price from dune.blockchains joined to prices.usd_with_native through
-- native_token_prices(), so currency_symbol resolves to 'USDC' and tx_fee is tx_fee_raw / 10^18.
{{ evm_l1_gas_fees(blockchain) }}
