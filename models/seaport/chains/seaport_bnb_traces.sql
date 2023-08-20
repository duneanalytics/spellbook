{{ config(
        tags = ['dunesql'],
        schema = 'seaport_bnb',
        alias =alias('traces'),
        partition_by=['block_date'],
        materialized='incremental',
        file_format = 'delta',
        unique_key = ['block_number', 'order_hash', 'trace_side', 'trace_index'],
        post_hook='{{ expose_spells(\'["bnb"]\',
                                "project",
                                "seaport",
                                \'["hildobby"]\') }}'
        )
}}

{{seaport_traces(
    blockchain='bnb'
    , seaport_events = source('seaport_bnb', 'Seaport_evt_OrderFulfilled')
)}}