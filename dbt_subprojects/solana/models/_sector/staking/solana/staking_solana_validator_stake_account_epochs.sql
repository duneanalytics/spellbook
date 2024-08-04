{{ config(
    schema = 'staking_solana'
    , alias = 'validator_stake_account_epochs'
    , materialized = 'table'
    , file_format = 'delta'
    , unique_key = ['epoch', 'stake_account', 'vote_account']
    , post_hook='{{ expose_spells(\'["solana"]\',
                                "sector",
                                "staking",
                                \'["ilemi"]\') }}')
}}

--https://dune.com/queries/3267749
--3962860 

--we want to get all epochs and the latest delegated vote before epoch started. then get the rewards from accounts per epoch.
--need to debug the empty vote accounts latest to check they are indeed inactive (order by staked amount)
  --also debug empty block_slots
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
            , row_number() over (partition by vote.stake_account order by vote.block_time desc) as last_delegation
        FROM {{ ref('staking_solana_stake_account_delegations') }} vote
        LEFT JOIN {{ ref('solana_utils_epochs') }} epoch ON first_block_epoch = true --cross join
        WHERE vote.block_slot< epoch.block_slot --only get changes to accounts before start of epoch
        -- no incremental since we technically need incremental by epoch not by time/day
    )

    , bal as (
        --get all stake accounts for each epoch and their balance
        SELECT
            b.epoch
            , b.epoch_time
            , b.epoch_start_slot
            , b.epoch_end_slot
            , b.stake_account
            , b.vote_account
            , bal.sol_balance
            , bal.day
            , row_number() over (partition by b.stake_account order by b.day desc) as latest_bal
        FROM base b
        LEFT JOIN solana_utils.daily_balances bal ON bal.address = b.stake_account 
            AND bal.token_mint_address is null
            AND bal.day <= date_trunc('day', b.epoch_time)
        WHERE b.last_delegation = 1
        and b.vote_account is not null
    )

SELECT 
    epoch
    , epoch_time
    , epoch_start_slot
    , epoch_end_slot
    , stake_account
    , vote_account
    , sol_balance
FROM bal
WHERE latest_bal = 1