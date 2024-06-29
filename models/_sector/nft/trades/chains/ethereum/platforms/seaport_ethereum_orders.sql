{{ config(
    schema = 'seaport_ethereum',
    alias = 'orders',
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
