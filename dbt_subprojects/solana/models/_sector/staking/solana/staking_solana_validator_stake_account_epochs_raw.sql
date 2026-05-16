{{ config(
    schema = 'staking_solana'
    , alias = 'validator_stake_account_epochs_raw'
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['epoch', 'stake_account']
    , incremental_predicates = ['DBT_INTERNAL_DEST.epoch_time >= now() - interval \'10\' day']
)
}}

--don't expose this table in DE
with
    base as (
        SELECT
            epoch.epoch
            , epoch.block_time as epoch_time
            , epoch.epoch_start_slot
            , epoch.epoch_next_start_slot
            , vote.stake_account
            , max_by(vote.vote_account, vote.block_time) as vote_account --latest delegated vote before epoch started. then get the rewards from accounts per epoch.
        FROM {{ ref('staking_solana_stake_account_delegations') }} vote
        LEFT JOIN {{ ref('solana_utils_epochs') }} epoch
            ON first_block_epoch = true --cross join
        WHERE vote.block_slot < epoch.block_slot --only get changes to accounts before start of epoch
        {% if is_incremental() %}
        -- Once an epoch starts, its (epoch_start_slot) cutoff is immutable.
        -- Reprocess the last ~4-5 epochs (epoch p99 = 57h, max ~60h) on each run
        -- so we re-emit the in-flight epoch and absorb late-arriving delegations.
        AND epoch.block_time >= now() - interval '10' day
        {% endif %}
        GROUP BY 1,2,3,4,5
    )

SELECT
    epoch
    , epoch_time
    , epoch_start_slot
    , epoch_next_start_slot
    , stake_account
    , vote_account
FROM base
WHERE vote_account is not null
