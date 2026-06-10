{{
    config(
        schema = 'openocean_avalanche_c',
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
        blockchain = 'avalanche_c',
        evt_swapped = source('openocean_v2_avalanche_c', 'OpenOceanExchange_evt_Swapped'),
        burn_addresses = ['0x0000000000000000000000000000000000000000'],
        w_native = '0xb31f66aa3c1e785363f0875a1b74e27b85fd66c7',
        project_start_date = '2021-09-09'
    )
}}
