{% set chain = 'ethereum' %}

{{
  config(
    schema = 'stablecoins_' ~ chain,
    alias = 'core_transfers',
    materialized = 'incremental',
    incremental_strategy = 'merge',
    partition_by = ['block_month'],
    unique_key = ['block_month', 'block_date', 'unique_key'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')]
  )
}}

-- core transfers: tracks transfers for stablecoins in the frozen core list
-- wraps the macro output in a CTE to exclude anomalous exploit transactions
-- context: https://github.com/duneanalytics/curated-stablecoins/pull/114

with base as (
    {{ stablecoins_transfers(
        blockchain = chain,
        token_list = 'core'
    ) }}
)

select *
from base
where tx_hash not in (
    0xaa532ae7f06cccdbdc226f59b68733ae8594464a98e128365f8170e305c34f4b
    ,0xc45dd1a77c05d9ae5b2284eea5393ecce2ac8a7e88e973c6ba3fe7a18bf45634
    ,0xb23aaecc086996f8059a0de67819b0e45d82e68c2581edf889b3177d7aa96ea6
    ,0x06712214f077b5bfd9568ca0d32da8633e1ea12bf3522a86195daaa0defe3977
)
