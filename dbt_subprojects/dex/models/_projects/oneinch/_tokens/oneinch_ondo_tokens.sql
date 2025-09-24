{{
    config(
        schema = 'oneinch',
        alias = 'ondo_tokens',
        materialized = 'view',
        unique_key = ['blockchain', 'contract_address']
    )
}}



-- currently view as just one source table and it's light (100 rows), if more sources added in future we can switch to table / incremental


select * from (
    select
        'ethereum' as blockchain
        , proxy as contract_address
        , ticker as symbol
        , name
        , evt_block_time as block_time 
        , row_number() over (partition by blockchain, proxy order by evt_block_time desc) as rn
    from {{ source('ondo_finance_ethereum', 'GMTokenFactory_evt_NewGMTokenDeployed') }}
)
where rn = 1 -- take latest event only in case of re-deployments