{{ 
    config (
        schema = 'tokens_celo',
        alias = 'balances_daily_agg_base',
        file_format = 'delta',
        materialized = 'incremental',
        incremental_strategy = 'merge',
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
        unique_key = ['day', 'unique_key'],
        partition_by = ['day'],
    )
}}

-- celo doesn't have a raw balances source like other chains
-- this uses the transfer-based balance calculation macro

{{
    balances_daily_agg_from_transfers(
        transfers = ref('tokens_celo_transfers'),
        gas_fees_source = source('gas_celo', 'fees')
    )
}}
