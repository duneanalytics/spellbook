{{ config(
        tags = ['dunesql'],
        schema = 'seaport_avalanche_c',
        alias =alias('traces'),
        partition_by=['block_date'],
        materialized='incremental',
        file_format = 'delta',
        unique_key = ['block_number', 'tx_hash', 'order_hash', 'trace_side', 'trace_index'],
        post_hook='{{ expose_spells(\'["avalanche_c"]\',
                                "project",
                                "seaport",
                                \'["hildobby"]\') }}'
        )
}}

{{seaport_traces(
    blockchain='avalanche_c'
    , seaport_events = source('seaport_avalanche_c', 'Seaport_evt_OrderFulfilled')
)}}