{% set blockchain = 'arbitrum' %}



{{ 
    config( 
        schema = 'oneinch_' + blockchain,
        alias = 'ar',
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['blockchain', 'tx_hash', 'call_trace_address']
    )
}}


with ar as (
    {{ 
        oneinch_ar_macro(
            blockchain = blockchain
        )
    }}
)

select *
from ar
where tx_hash != 0x6d36e922c7885c9d2d2cd57ef1cc9d47f0aefad9331c7d2493c43971d0e06816