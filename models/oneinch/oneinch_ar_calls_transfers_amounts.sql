{{  
    config(
        schema = 'oneinch',
        alias = alias('ar_calls_transfers_amounts'),
        materialized = 'view',
        unique_key = ['blockchain', 'unique_call_transfer_id'],
        tags = ['dunesql'],
    )
}}

-- TEMP TABLE, WILL BE REMOVED AS SOON AS WE MIGRATE DUNE QUERIES


select * from {{ ref('oneinch_calls_transfers_amounts') }}
where 
    protocol = 'AR'
    and (
        blockchain != 'bnb'
        or
        blockchain = 'bnb' and (rn_tta_asc <= 2 or rn_tta_desc <= 2)
    )




