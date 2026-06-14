{{
    config(
        schema = 'openocean_fantom',
        alias = 'base_trades',
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['block_date', 'blockchain', 'project', 'version', 'tx_hash', 'evt_index', 'trace_address'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

{{
    openocean_compatible_v2_trades(
        blockchain = 'fantom',
        evt_swapped = source('open_ocean_fantom', 'OpenOceanExchange_evt_Swapped'),
        burn_addresses = ['0x0000000000000000000000000000000000000000'],
        w_native = '0x21be370d5312f44cb42ce377bc9b8a0cef1a4c83'
    )
}}
