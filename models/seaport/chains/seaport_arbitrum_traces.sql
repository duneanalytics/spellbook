{{ config(
        tags = ['dunesql'],
        schema = 'seaport_arbitrum',
        alias =alias('traces'),
        partition_by=['block_date'],
        materialized='incremental',
        file_format = 'delta',
        unique_key = ['block_number', 'tx_hash', 'order_hash', 'trace_side', 'trace_index', 'identifier'],
        post_hook='{{ expose_spells(\'["arbitrum"]\',
                                "project",
                                "seaport",
                                \'["hildobby"]\') }}'
        )
}}

{{seaport_traces(
    blockchain='arbitrum'
    , seaport_events =  source('seaport_arbitrum', 'Seaport_evt_OrderFulfilled')
)}}