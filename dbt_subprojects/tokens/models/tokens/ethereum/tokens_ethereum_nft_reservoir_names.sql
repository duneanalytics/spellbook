{{ config(
        schema = 'tokens_ethereum'
        , alias = 'nft_reservoir_names'
        , materialized = 'incremental'
        , file_format = 'delta'
        , incremental_strategy = 'merge'
        , unique_key = 'contract_address'
        , post_hook = '{{ hide_spells() }}'
        )
}}

-- One-time snapshot of the DEPRECATED reservoir community dataset (source frozen 2025-05-28,
-- no updates in >1 year). Materializing earliest-name-per-contract here lets tokens_ethereum_nft
-- read a small table instead of re-scanning ~31 GB of frozen reservoir data on every run.
-- The is_incremental() guard makes every run after the first build a no-op. If the reservoir
-- dataset is ever revived, rebuild with --full-refresh.
select
    contract as contract_address
  , min_by(name, created_at) as name
from {{ source('reservoir', 'collections') }}
{% if is_incremental() %}
where false
{% endif %}
group by 1
