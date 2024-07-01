{{
  config(
        schema = 'orca_whirlpool',
        alias = 'token_prices',
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.minute')],
        unique_key = ['blockchain', 'contract_address', 'minute'],
        pre_hook='{{ enforce_join_distribution("PARTITIONED") }}',
        post_hook='{{ expose_spells(\'["solana"]\',
                                    "project",
                                    "orca_whirlpool",
                                    \'["get_nimbus"]\') }}')
}}

{# { % set project_start_date = '2022-03-10' % } --grabbed min block time from whirlpool_solana.whirlpool_call_swap #}
with
    raw_data as (
        SELECT
            *
        FROM
            {{ ref('dex_solana_trades') }}
        WHERE 1 = 1
            AND project = 'whirlpool'
            {% if is_incremental() %}
            AND {{ incremental_predicate('block_time') }}
            {% else %}
            AND block_time >= DATE('2022-03-10')
            {% endif %}
    ),
    bought_price as (
        SELECT
            token_bought_mint_address as token_mint,
            DATE_TRUNC('minute', block_time) AS minute,
            SUM(amount_usd) / SUM(token_bought_amount) AS price
        FROM raw_data
        GROUP BY
            1,
            2
    ),
    sold_price as (
        SELECT token_sold_mint_address as token_mint,
            DATE_TRUNC('minute', block_time) AS minute,
            SUM(amount_usd) / SUM(token_sold_amount) AS price
        FROM raw_data
        GROUP BY 1,
            2
    ),
    all_trades as (
        SELECT *
        FROM bought_price
        UNION ALL
        SELECT *
        FROM sold_price
    )
SELECT t1.token_mint as contract_address,
    t1.minute as minute,
    t2.symbol,
    t2.decimals,
    'solana' as blockchain,
    avg(t1.price) as price,
    CAST(DATE_TRUNC('month', t1.minute) as date) as block_month
FROM all_trades t1
    JOIN
        {{ ref('tokens_solana_fungible') }}  t2 ON t1.token_mint = t2.token_mint_address
GROUP BY 1,
    2,
    3,
    4