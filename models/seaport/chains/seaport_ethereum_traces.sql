{{ config(
        tags = ['dunesql'],
        schema = 'seaport_ethereum',
        alias =alias('traces'),
        unique_key = ['block_number', 'tx_hash', 'evt_index', 'order_hash', 'trace_side', 'trace_index'],
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
