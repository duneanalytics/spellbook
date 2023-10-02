{{ config(
        tags = ['dunesql'],
        schema = 'seaport_base',
        alias =alias('traces'),
        unique_key = ['block_number', 'tx_hash', 'evt_index', 'order_hash', 'trace_side', 'trace_index']
        )
}}

{{seaport_traces(
    blockchain='base'
    , seaport_events = source('seaport_base', 'Seaport_evt_OrderFulfilled')
)}}
