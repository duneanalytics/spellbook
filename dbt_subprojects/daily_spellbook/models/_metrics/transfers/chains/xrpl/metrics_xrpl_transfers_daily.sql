{{ config(
        schema = 'metrics_xrpl'
        , alias = 'transfers_daily'
        , materialized = 'view'
        )
}}

-- Temporary placeholder while XRPL transfer outputs stay in the tokens rollout path.
select
    cast('xrpl' as varchar) as blockchain
    , cast(null as date) as block_date
    , cast(null as double) as net_transfer_amount_usd
where false