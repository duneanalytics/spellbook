{{
    config(
        schema = 'openocean_bnb',
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
        blockchain = 'bnb',
        evt_swapped = source('openocean_v2_bnb', 'OpenOceanExchange_evt_Swapped'),
        burn_addresses = ['0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee'],
        w_native = '0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c',
        project_start_date = '2021-09-18'
    )
}}
