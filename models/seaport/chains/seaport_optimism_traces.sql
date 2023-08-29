{{ config(
        tags = ['dunesql'],
        schema = 'seaport_optimism',
        alias =alias('traces'),
        unique_key = ['block_number', 'tx_hash', 'evt_index', 'order_hash', 'trace_side', 'trace_index'],
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
