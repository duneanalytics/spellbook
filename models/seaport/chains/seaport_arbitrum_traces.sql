{{ config(
        tags = ['dunesql'],
        schema = 'seaport_arbitrum',
        alias =alias('traces'),
        unique_key = ['block_number', 'tx_hash', 'evt_index', 'order_hash', 'trace_side', 'trace_index'],
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
