{{ config(
        tags = ['dunesql'],
        schema = 'seaport_ethereum',
        alias =alias('traces'),
        partition_by=['block_date'],
        materialized='incremental',
        file_format = 'delta',
        unique_key = ['block_number', 'order_hash', 'trace_side', 'trace_index', 'identifier'],
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                "project",
                                "seaport",
                                \'["hildobby"]\') }}'
        )
}}

{{seaport_traces(
    blockchain='ethereum'
    , seaport_events = source('seaport_ethereum', 'Seaport_evt_OrderFulfilled')
)}}