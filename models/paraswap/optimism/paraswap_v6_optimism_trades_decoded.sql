{{ config(
    schema = 'paraswap_v6_optimism',
    alias = 'trades_decoded',    
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'blockchain', 'project', 'version', 'txHash', 'method', 'callTraceAddress'],
    post_hook='{{ expose_spells(\'["optimism"]\',
                                "project",
                                "paraswap_v6",
                                \'["eptighte", "mwamedacen"]\') }}'
    )
}}

{{ paraswap_v6_trades_master('optimism', 'paraswap', 'AugustusV6') }}
