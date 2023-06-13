{{ config(
    schema = 'dex_ethereum',
    alias ='trades_beta',
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'project', 'version', 'tx_hash', 'evt_index', 'trace_address']
    )
}}