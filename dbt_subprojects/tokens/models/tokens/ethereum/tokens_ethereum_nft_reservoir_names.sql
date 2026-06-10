{{ config(
        schema = 'tokens_ethereum'
        , alias = 'nft_reservoir_names'
        , materialized = 'table'
        , file_format = 'delta'
        , tags = ['static']
        )
}}

-- One-time snapshot of the DEPRECATED reservoir community dataset (source frozen 2025-05-28,
-- no updates in >1 year). Materializing earliest-name-per-contract here lets tokens_ethereum_nft
-- read a small table instead of re-scanning ~31 GB of frozen reservoir data on every run.
-- Tagged static: builds only on deploy, never on the regular schedule. If the reservoir
-- dataset is ever revived, trigger a redeploy of this model to refresh the snapshot.
select
    contract as contract_address
  , min_by(name, created_at) as name
from {{ source('reservoir', 'collections') }}
group by 1
