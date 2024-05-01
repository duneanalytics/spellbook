{{
    config(
        schema = 'orca_whirlpool',
        alias = 'token_prices',
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['blockchain', 'contract_address', 'minute'],
        pre_hook = '{{ enforce_join_distribution("PARTITIONED") }}',
            post_hook='{{ expose_spells(\'["solana"]\',
                                        "project",
                                        "orca_whirlpool",
                                        \'["get_nimbus"]\') }}')

}}

{ % set project_start_date = '2022-03-10' % } --grabbed min block time from whirlpool_solana.whirlpool_call_swap
with
    raw as (
        select *
        FROM orca_whirlpool.trades
        WHERE 1 = 1 { % if is_incremental() % }
            AND { { incremental_predicate('minute') } } { %
            else % }
            AND block_time >= DATE('{{project_start_date}}') { % endif % }
    ),
    bought_price as (
        SELECT token_bought_mint_address as token_mint,
            DATE_TRUNC('minute', block_time) AS minute,
            SUM(amount_usd) / SUM(token_bought_amount) AS price
        FROM raw
        GROUP BY 1,
            2
    ),
    sold_price as (
        SELECT token_sold_mint_address as token_mint,
            DATE_TRUNC('minute', block_time) AS minute,
            SUM(amount_usd) / SUM(token_sold_amount) AS price
        FROM raw
        GROUP BY 1,
            2
    ),
    all_trades as (
        select *
        from bought_price
        UNION ALL
        select *
        from sold_price
    )
select t1.token_mint as contract_address,
    t1.minute as minute,
    t2.symbol,
    t2.decimals,
    'solana' as blockchain,
    avg(t1.price) as price,
    DATE_TRUNC('month', t1.minute) as block_month
from all_trades t1
    join tokens_solana.fungible t2 ON t1.token_mint = t2.token_mint_address
group by 1,
    2,
    3,
    4