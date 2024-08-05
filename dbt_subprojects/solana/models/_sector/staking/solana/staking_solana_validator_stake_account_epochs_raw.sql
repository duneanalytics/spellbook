{{ config(
    schema = 'staking_solana'
    , alias = 'validator_stake_account_epochs_raw'
    , materialized = 'table'
    , file_format = 'delta'
    , unique_key = ['epoch', 'stake_account', 'vote_account'])
}}

--don't expose this table in DE
--we want to get all epochs and the latest delegated vote before epoch started. then get the rewards from accounts per epoch.
with 
    base as (
        SELECT 
            epoch.epoch
            , epoch.block_time as epoch_time
            , epoch.epoch_start_slot
            , epoch.epoch_end_slot
            , vote.stake_account
            , vote.vote_account
            , vote.block_time as vote_block_time
            , vote.block_slot as vote_block_slot
            , row_number() over (partition by epoch.epoch, vote.stake_account order by vote.block_time desc) as last_delegation_epoch
        FROM {{ ref('staking_solana_stake_account_delegations') }} vote
        LEFT JOIN {{ ref('solana_utils_epochs') }} epoch ON first_block_epoch = true --cross join
        WHERE vote.block_slot < epoch.block_slot --only get changes to accounts before start of epoch
        -- no incremental since we technically need incremental by epoch not by time/day, fix later
    )

SELECT
    epoch
    , epoch_time
    , bpoch_start_slot
    , epoch_end_slot
    , stake_account
    , vote_account
FROM base
WHERE last_delegation_epoch = 1
and vote_account is not null