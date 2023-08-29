{{ config(
        tags = ['dunesql'],
        schema = 'seaport_base',
        alias =alias('traces'),
        unique_key = ['block_number', 'tx_hash', 'evt_index', 'order_hash', 'trace_side', 'trace_index'],
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
