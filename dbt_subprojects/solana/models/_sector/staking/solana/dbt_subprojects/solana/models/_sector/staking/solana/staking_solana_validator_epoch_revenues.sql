-- migrating @ilemi materialized view to spellbook

{{ config(
    schema = 'staking_solana'
    , alias = 'validator_revenues'
    , materialized = 'table'
    , file_format = 'delta'
    , unique_key = ['epoch','vote_account']
)
}}

-- don't expose this table in DE until validator fees are added

with 
    commission as (
        SELECT 
            floor(cast(block_slot as double) / 432000) - 1 as epoch --commission paid out at start of next epoch
            , recipient as vote_account
            , cast(commission as double)/100 as stake_commission_rate
            , sum(lamports/1e9) as stake_commission_rewards
        FROM {{ source('solana', 'rewards') }} 
        WHERE reward_type = 'Voting' 
        GROUP BY 1,2,3
    )
    
    , stake as (
        SELECT 
            vote.epoch
            , vote.vote_account
            , sum(lamports/1e9) as stake_account_rewards
            , count(distinct recipient) as stake_accounts
        FROM {{ source('solana', 'rewards') }} rew
        JOIN staking_solana.validator_stake_account_epochs vote
            ON vote.stake_account = rew.recipient
            AND rew.block_slot = vote.epoch_next_start_slot 
        WHERE reward_type = 'Staking'
        GROUP BY 1,2
    )
    
    , mev_account as (
        SELECT
            vote.epoch
            , vote.vote_account
            , sum(amount/1e9) as mev_account_rewards
        FROM {{ ref('jito_tip_distribution_solana', 'jito_tip_distribution_call_claim') }} c
        JOIN staking_solana.validator_stake_account_epochs vote
            ON vote.stake_account = c.account_claimant
            AND c.call_block_slot >= vote.epoch_start_slot
            AND c.call_block_slot < COALESCE(vote.epoch_next_start_slot,vote.epoch_start_slot+432000)
        GROUP BY 1,2
    )
    
    , mev_com as (
        SELECT
            floor(cast(c.call_block_slot as double) / 432000) as epoch
            , c.account_claimant as vote_account 
            , sum(amount/1e9) as mev_commission_rewards
        FROM {{ ref('jito_tip_distribution_solana', 'jito_tip_distribution_call_claim') }} c
        WHERE c.account_claimant IN (SELECT distinct vote_account FROM {{ source('staking_solana', 'validator_stake_account_epochs') }})
        GROUP BY 1,2
    )

SELECT 
  COALESCE(c.epoch, s.epoch) as epoch 
  , COALESCE(c.vote_account, s.vote_account) as vote_account 
  , s.stake_accounts
  , c.stake_commission_rate
  , c.stake_commission_rewards
  , s.stake_account_rewards
  , cast(COALESCE(mc.mev_commission_rewards, 0) as double)/cast(ma.mev_account_rewards + COALESCE(mc.mev_commission_rewards, 0) as double) as mev_commission_rate
  , COALESCE(mc.mev_commission_rewards, 0) as mev_commission_rewards
  , ma.mev_account_rewards
FROM commission c
FULL OUTER JOIN stake s ON c.epoch = s.epoch AND c.vote_account = s.vote_account
FULL OUTER JOIN mev_account ma ON c.epoch = ma.epoch AND c.vote_account = ma.vote_account
FULL OUTER JOIN mev_com mc ON c.epoch = mc.epoch AND c.vote_account = mc.vote_account
ORDER BY 1 desc
