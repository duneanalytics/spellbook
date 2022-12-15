{{ config(
        alias='bep20_hour',
        partition_by = ['hour'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['hour', 'wallet_address', 'token_address'],
        post_hook='{{ expose_spells_hide_trino(\'["bnb"]\',
                                            "sector",
                                            "balances",
                                            \'["crypto586"]\') }}'
        )
}}

WITH
hours AS (
    -- BSC mainnet launch date
    SELECT explode(sequence(to_date('2020-08-31'), date_trunc('hour', now()), interval 1 hour)) AS hour
),

hourly_balances AS (
    SELECT
        wallet_address,
        token_address,
        amount_raw,
        amount,
        hour,
        symbol,
        lead(hour, 1, now()) OVER (PARTITION BY token_address, wallet_address ORDER BY hour) AS next_hour
    FROM
        {{ ref('transfers_bnb_bep20_rolling_hour') }}
    {% if is_incremental() %}
    WHERE hour >= date_trunc("day", now() - interval '1 week')
    {% endif %}
)

SELECT
    'bnb' as blockchain,
    h.hour,
    b.wallet_address,
    b.token_address,
    b.amount_raw,
    b.amount,
    b.amount * p.price as amount_usd,
    b.symbol
FROM
    hourly_balances b
INNER JOIN
    hours h ON b.hour <= h.hour AND h.hour < b.next_hour

LEFT JOIN
    {{ source('prices', 'usd') }} p ON p.contract_address = b.token_address
    AND h.hour = p.minute
    AND p.blockchain = 'bnb'

-- Removes rebase tokens from balances
LEFT JOIN {{ ref('tokens_bnb_rebase') }} AS r ON b.token_address = r.contract_address

-- Removes likely non-compliant tokens due to negative balances
LEFT JOIN {{ ref('balances_bnb_bep20_noncompliant') }} AS nc ON b.token_address = nc.token_address

WHERE
    r.contract_address IS NULL AND nc.token_address IS NULL
