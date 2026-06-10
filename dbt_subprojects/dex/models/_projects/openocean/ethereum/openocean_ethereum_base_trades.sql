{{
    config(
        schema = 'openocean_ethereum',
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
        blockchain = 'ethereum',
        evt_swapped = source('openocean_v2_ethereum', 'OpenOceanExchangeProxy_evt_Swapped'),
        burn_addresses = ['0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee'],
        w_native = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2',
        project_start_date = '2021-10-17'
    )
}}
