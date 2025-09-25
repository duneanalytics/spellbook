{{
    config(
        schema = 'oneinch',
        alias = 'ondo_tokens',
        materialized = 'view',
        unique_key = ['blockchain', 'contract_address']
    )
}}



-- currently view as just one source table and it's light (100 rows), if more sources added in future we can switch to table / incremental
select
    'ethereum' as blockchain
    , proxy as contract_address
    , max_by(ticker, evt_block_time) as symbol
    , max_by(name, evt_block_time)
    , max_by(evt_block_time, evt_block_time) as block_time 
from {{ source('ondo_finance_ethereum', 'GMTokenFactory_evt_NewGMTokenDeployed') }}
group by 1, 2 -- take latest event only in case of re-deployments