{{ config(
    schema = 'paraswap_v6_polygon',
    alias = 'trades_decoded',    
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'blockchain', 'project', 'version', 'txHash', 'method', 'callTraceAddress'],
    post_hook='{{ expose_spells(\'["polygon"]\',
                                "project",
                                "paraswap_v6",
                                \'["eptighte", "mwamedacen"]\') }}'
    )
}}

{{ paraswap_v6_trades_master('polygon', 'paraswap', 'AugustusV6') }}