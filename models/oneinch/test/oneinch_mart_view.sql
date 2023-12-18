{% set blockchain = 'arbitrum' %}



{{ 
    config( 
        schema = 'oneinch',
        alias = 'mart_view',
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['tx_hash', 'transfer_trace_address']
    )
}}


select * from (
    {{ 
        oneinch_test_macro(
            blockchain = blockchain
        )
    }}
)
where block_time >= (
    select first_deploy_at from {{ ref('oneinch_dict_view')}}
)
