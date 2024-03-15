{{ config(
    schema = 'paraswap_v6_base',
    alias = 'trades_decoded',    
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'blockchain', 'project', 'version', 'txHash', 'method', 'callTraceAddress']    
    )
}}

{{ paraswap_v6_trades_master('base', 'paraswap', 'AugustusV6') }}
