{{ config(
    schema = 'staking_solana'
    , alias = 'validator_stake_account_epochs'
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['epoch', 'stake_account', 'vote_account']
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
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
    )

--get all stake accounts for each epoch
SELECT
    epoch
    , epoch_time
    , epoch_start_slot
    , epoch_end_slot
    , stake_account
    , vote_account
FROM base
WHERE last_delegation = 1
and vote_account is not null