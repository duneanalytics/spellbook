{{ config(
    schema = 'seaport_ethereum',
    alias = 'test',
    materialized = 'table',
    file_format = 'delta',
    )
}}

{{
seaport_orders(
             source('seaport_ethereum','Seaport_evt_OrderFulfilled')
            ,source('seaport_ethereum','Seaport_evt_OrdersMatched')
            )
}}
