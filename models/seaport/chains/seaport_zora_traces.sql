{{ config(
        
        schema = 'seaport_zora',
        alias ='traces',
        unique_key = ['block_number', 'tx_hash', 'evt_index', 'order_hash', 'trace_side', 'trace_index']
        )
}}

{{seaport_traces(
    blockchain='zora'
    , seaport_events = source('seaport_zora', 'Seaport_evt_OrderFulfilled')
)}}
