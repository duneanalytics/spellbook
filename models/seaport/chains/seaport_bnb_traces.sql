{{ config(
        tags = ['dunesql'],
        schema = 'seaport_bnb',
        alias =alias('traces'),
        unique_key = ['block_number', 'tx_hash', 'evt_index', 'order_hash', 'trace_side', 'trace_index']
        )
}}

{{seaport_traces(
    blockchain='bnb'
    , seaport_events = source('seaport_bnb', 'Seaport_evt_OrderFulfilled')
)}}
