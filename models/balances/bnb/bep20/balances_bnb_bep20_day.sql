{{ config(
        alias='bep20_day',
        partition_by = ['day'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['day', 'wallet_address', 'token_address'],
        post_hook='{{ expose_spells_hide_trino(\'["bnb"]\',
                                            "sector",
                                            "balances",
                                            \'["crypto586"]\') }}'
        )
}}

WITH

days AS (
     -- BSC mainnet launch date
    {% if not is_incremental() %}
    SELECT explode(sequence(to_date('2020-08-31'), date_trunc('day', now()), interval 1 day)) AS day
    {% endif %}
    {% if is_incremental() %}
    SELECT explode(sequence(date_trunc("day", now() - interval '1 week'), date_trunc('day', now()), interval 1 day)) AS day
    {% endif %}
),

daily_balances AS (
    SELECT
        tr.wallet_address,
        tr.token_address,
        tr.amount_raw,
        tr.amount,
        tr.day,
        tr.symbol,
        lead(day, 1, now()) OVER (PARTITION BY tr.token_address, tr.wallet_address ORDER BY day) AS next_day
    FROM
        {{ ref('transfers_bnb_bep20_rolling_day') }} AS tr
    INNER JOIN
        {{ ref('prices_tokens') }} AS t
        ON tr.token_address = t.contract_address
        AND t.blockchain = 'bnb'
    {% if is_incremental() %}
    WHERE tr.day >= date_trunc("day", now() - interval '1 week')
    {% endif %}
),

full_calenddr_daily_balances as (
    select
        date_format(date_add(b.`day`, i), 'yyyy-MM-dd') as `day`,
        wallet_address,
        token_address,
        amount_raw,
        amount,
        symbol
    from daily_balances b
        lateral view outer
        posexplode(split(space(datediff(next_day, b.`day`)), ' ')) temp as i,x
    where to_date('2020-08-31') <= b.day and b.day <= current_date
)

SELECT
    'bnb' AS blockchain,
    b.day,
    b.wallet_address,
    b.token_address,
    b.amount_raw,
    b.amount,
    b.amount * p.price AS amount_usd,
    b.symbol
FROM
    full_calenddr_daily_balances b
LEFT JOIN
    {{ source('prices', 'usd') }} AS p ON p.contract_address = b.token_address
        AND b.day = p.minute
        AND p.blockchain = 'bnb'