{{ config(
        tags = ['dunesql', 'prod_exclude'],
        schema = 'seaport_base',
        alias =alias('traces'),
        partition_by=['block_date'],
        materialized='incremental',
        file_format = 'delta',
        unique_key = ['block_number', 'order_hash', 'trace_side', 'trace_index', 'identifier'],
        post_hook='{{ expose_spells(\'["base"]\',
                                "project",
                                "seaport",
                                \'["hildobby"]\') }}'
        )
}}

{{seaport_traces(
    blockchain='base'
    , seaport_events = source('seaport_base', 'Seaport_evt_OrderFulfilled')
)}}