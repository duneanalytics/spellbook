{{ config(
        tags = ['dunesql'],
        schema = 'seaport_optimism',
        alias =alias('traces'),
        partition_by=['block_date'],
        materialized='incremental',
        file_format = 'delta',
        unique_key = ['block_number', 'tx_hash', 'order_hash', 'trace_side', 'trace_index', 'identifier', 'recipient', 'offerer'],
        post_hook='{{ expose_spells(\'["optimism"]\',
                                "project",
                                "seaport",
                                \'["hildobby"]\') }}'
        )
}}

{{seaport_traces(
    blockchain='optimism'
    , seaport_events = source('seaport_optimism', 'Seaport_evt_OrderFulfilled')
)}}