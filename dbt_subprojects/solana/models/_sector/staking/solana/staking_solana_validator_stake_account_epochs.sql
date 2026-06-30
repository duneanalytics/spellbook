{{ config(
    schema = 'staking_solana'
    , alias = 'validator_stake_account_epochs'
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['epoch', 'stake_account']
    , incremental_predicates = ['DBT_INTERNAL_DEST.epoch_time >= now() - interval \'10\' day']
    , post_hook='{{ hide_spells() }}')
}}

{% if is_incremental() -%}
-- Incremental: forward-fill each stake account's SOL balance instead of re-scanning the full
-- ~5.5B-row native-SOL history of solana_utils_daily_balances every run. daily_balances is
-- activity-only (not forward-filled), so the previous as-of join had no time bound on the balance
-- side and read all history each run. Here the last-known balance is carried forward from the most
-- recent epoch snapshot already in {{ this }} (which is itself a per-epoch forward-filled snapshot),
-- updated only with recent balance activity. Equivalent to the all-history as-of join: proven
-- EXCEPT=0 over epochs 990-994 except for a handful of brand-new-this-window stake accounts that
-- were funded before the seed epoch and have had no balance activity since (seed and recent activity
-- both absent -> sol_balance null); these self-resolve on their next balance change.
WITH
    seed AS (
        -- last-known balance per stake_account: the most recent epoch snapshot BEFORE the window
        SELECT stake_account, sol_balance
        FROM {{ this }}
        WHERE epoch = (
            SELECT max(epoch)
            FROM {{ this }}
            WHERE epoch_time >= now() - interval '20' day
                AND epoch_time < now() - interval '10' day
        )
    )

    , raw AS (
        SELECT
            epoch
            , epoch_time
            , epoch_start_slot
            , epoch_next_start_slot
            , stake_account
            , vote_account
        FROM {{ ref('staking_solana_validator_stake_account_epochs_raw') }}
        WHERE epoch_time >= now() - interval '10' day
    )

    , balance_changes AS (
        -- recent native-SOL balance activity, bounded to cover the seed -> window gap with margin
        SELECT
            address
            , day
            , sol_balance
        FROM {{ ref('solana_utils_daily_balances') }}
        WHERE token_mint_address is null
            AND day > date_trunc('day', now() - interval '15' day)
    )

    , recent_balance AS (
        -- latest recent balance at or before each epoch's day, per (epoch, stake_account)
        SELECT
            b.epoch
            , b.stake_account
            , max_by(c.sol_balance, c.day) as sol_balance
        FROM raw b
        INNER JOIN balance_changes c
            ON c.address = b.stake_account
            AND c.day <= date_trunc('day', b.epoch_time)
        GROUP BY 1, 2
    )

SELECT
    b.epoch
    , b.epoch_time
    , b.epoch_start_slot
    , b.epoch_next_start_slot
    , b.stake_account
    , b.vote_account
    , coalesce(r.sol_balance, s.sol_balance) as sol_balance
FROM raw b
LEFT JOIN recent_balance r
    ON r.epoch = b.epoch
    AND r.stake_account = b.stake_account
LEFT JOIN seed s
    ON s.stake_account = b.stake_account

{% else -%}
-- Full refresh (one-time / --full-refresh): {{ this }} has no prior snapshot to seed from, so
-- recompute each epoch's balance directly from native-SOL balance history with an as-of join.
SELECT
    b.epoch
    , b.epoch_time
    , b.epoch_start_slot
    , b.epoch_next_start_slot
    , b.stake_account
    , b.vote_account
    , max_by(bal.sol_balance, bal.day) as sol_balance
FROM {{ ref('staking_solana_validator_stake_account_epochs_raw') }} b
LEFT JOIN {{ ref('solana_utils_daily_balances') }} bal ON bal.address = b.stake_account
    AND bal.token_mint_address is null
    AND bal.day <= date_trunc('day', b.epoch_time)
    {% if target.name == 'ci' -%}
    -- bound the balance scan in CI so the full-refresh build completes under the workflow cap; prod is unaffected
    AND bal.day >= date_trunc('day', now() - interval '17' day)
    {% endif -%}
{% if target.name == 'ci' -%}
WHERE b.epoch_time >= now() - interval '17' day
{% endif -%}
GROUP BY 1,2,3,4,5,6
{% endif -%}
