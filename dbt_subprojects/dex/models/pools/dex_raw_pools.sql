{{ config(
        schema='dex',
        alias = 'raw_pools',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.creation_block_time')],
        unique_key = ['blockchain', 'pool']
        , post_hook='{{ hide_spells() }}'
)
}}



-- get rid of degen-pools changed tokens after creation
select * from {{ ref('dex_raw_pool_creations') }}
where (blockchain, pool) not in (
    select blockchain, pool from {{ ref('dex_raw_pool_initializations') }}
    group by blockchain, pool
    having count(*) > 1
)
{% if is_incremental() %}
    and {{incremental_predicate('creation_block_time')}}
{% endif %}
