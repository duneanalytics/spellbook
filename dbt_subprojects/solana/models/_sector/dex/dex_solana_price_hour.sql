{{
  config(
        schema = 'dex_solana',
        alias = 'price_hour',
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.hour')],
        unique_key = ['blockchain', 'contract_address', 'hour'],
        pre_hook='{{ enforce_join_distribution("PARTITIONED") }}',
        post_hook='{{ expose_spells(\'["solana"]\',
                            "sector",
                            "dex_solana",
                            \'["get_nimbus"]\') }}')
}}

{% set project_start_date = '2022-03-10' %} --grabbed min block time from whirlpool_solana.whirlpool_call_swap
with
    raw_data as (
        SELECT
            blockchain
            , project
            , version
            , block_month
            , block_date
            , block_time
            , block_slot
            , trade_source
            , token_bought_symbol
            , token_sold_symbol
            , token_pair
            , token_bought_amount
            , token_sold_amount
            , token_bought_amount_raw
            , token_sold_amount_raw
            , amount_usd
            , fee_tier
            , fee_usd
            , token_bought_mint_address
            , token_sold_mint_address
            , token_bought_vault
            , token_sold_vault
            , project_program_id
            , project_main_id
            , trader_id
            , tx_id
            , outer_instruction_index
            , inner_instruction_index
            , tx_index
        FROM
            {{ ref('dex_solana_trades') }}
        WHERE 1 = 1
            {% if is_incremental() %}
            AND {{ incremental_predicate('block_time') }}
            {% else %}
            AND block_time >= DATE '{{project_start_date}}'
            {% endif %}
    ),
    bought_price as (
        SELECT
            token_bought_mint_address as token_mint,
            DATE_TRUNC('hour', block_time) AS hour,
            SUM(amount_usd) / SUM(token_bought_amount) AS price
        FROM raw_data
        GROUP BY
            1,
            2
    ),
    sold_price as (
        SELECT token_sold_mint_address as token_mint,
            DATE_TRUNC('hour', block_time) AS hour,
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
    t1.hour as hour,
    t2.symbol,
    t2.decimals,
    'solana' as blockchain,
    avg(t1.price) as price,
    CAST(DATE_TRUNC('month', t1.hour) as date) as block_month
FROM all_trades t1
    JOIN
        {{ source('tokens_solana','fungible') }}  t2 ON t1.token_mint = t2.token_mint_address
GROUP BY 1,
    2,
    3,
    4