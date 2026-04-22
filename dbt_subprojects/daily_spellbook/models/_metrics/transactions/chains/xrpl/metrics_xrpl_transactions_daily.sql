{{
    config(
        schema = 'metrics_xrpl',
        alias = 'transactions_daily',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['blockchain', 'block_date'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')]
    )
}}

-- Temporary placeholder while XRPL transfer outputs stay in the tokens rollout path.
select
    cast('xrpl' as varchar) as blockchain
    , cast(null as date) as block_date
    , cast(null as bigint) as tx_count
where false