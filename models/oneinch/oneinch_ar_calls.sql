{{  
    config(
        schema = 'oneinch',
        alias = alias('ar_calls'),
        materialized = 'view',
        unique_key = ['blockchain', 'tx_hash', 'call_trace_address'],
        tags = ['dunesql'],
    )
}}

-- TEMP TABLE, WILL BE REMOVED AS SOON AS WE MIGRATE DUNE QUERIES

select * from {{ ref('oneinch_calls') }}
where protocol = 'AR'
