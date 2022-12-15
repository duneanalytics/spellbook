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
    SELECT explode(sequence(to_date('2020-08-31'), date_trunc('day', now()), interval 1 day)) AS day
),

daily_balances AS (
    SELECT
        wallet_address,
        token_address,
        amount_raw,
        amount,
        day,
        symbol,
        lead(day, 1, now()) OVER (PARTITION BY token_address, wallet_address ORDER BY day) AS next_day
    FROM
        {{ ref('transfers_bnb_bep20_rolling_day') }}
    {% if is_incremental() %}
    WHERE day >= date_trunc("day", now() - interval '1 week')
    {% endif %}
)

SELECT
    'bnb' AS blockchain,
    d.day,
    b.wallet_address,
    b.token_address,
    b.amount_raw,
    b.amount,
    b.amount * p.price AS amount_usd,
    b.symbol
FROM
    daily_balances b
INNER JOIN
    days d ON b.day <= d.day
        AND d.day < b.next_day

LEFT JOIN
    {{ source('prices', 'usd') }} p ON p.contract_address = b.token_address
        AND d.day = p.minute
        AND p.blockchain = 'bnb'

-- Removes rebase tokens from balances (I don't know why actually. I kept the logic from balances_ethereum_erc20_day).
LEFT JOIN
    {{ ref('tokens_bnb_rebase') }} AS r ON b.token_address = r.contract_address

-- Removes likely non-compliant tokens due to negative balances
LEFT JOIN
    {{ ref('balances_bnb_bep20_noncompliant') }} AS nc ON b.token_address = nc.token_address

WHERE
    r.contract_address IS NULL AND nc.token_address IS NULL
