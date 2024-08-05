{{ config(
    schema = 'staking_solana'
    , alias = 'stake_account_delegations'
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['block_time', 'block_slot', 'stake_account', 'vote_account']
    , post_hook='{{ expose_spells(\'["solana"]\',
                                "sector",
                                "staking",
                                \'["ilemi"]\') }}')
}}

with 
    all_delegates as (
        --get all stake accounts, and get all of the vote accounts they have been delegated to over time
        SELECT
            distinct
            account_stakeAccount
            , account_voteAccount
            , call_block_time
            , call_block_slot
        FROM (
            SELECT 
                account_stakeAccount
                , account_voteAccount
                , COALESCE(call_block_time, timestamp '1990-01-01 00:00:00') as call_block_time --fill in nulls, at a fake earlier date than split thing below
                , call_block_slot
            FROM {{ source ('stake_program_solana','stake_call_DelegateStake') }}
            {% if is_incremental() %}
            WHERE {{incremental_predicate('call_block_time')}}
            {% endif %}

            UNION ALL
            
            --SPLIT creates a new account, but copies vote delegation from original. We get that and also look for the latest delegation of the account.
            SELECT 
                a.destination as account_stakeAccount
                , COALESCE(del.account_voteAccount, old_vote.account_voteAccount) as account_voteAccount
                , COALESCE(del.call_block_time, old_vote.call_block_time) as call_block_time
                , COALESCE(del.call_block_slot, old_vote.call_block_slot) as call_block_slot
            FROM {{ ref('staking_solana_stake_actions') }} a
            --get inherited vote account
            LEFT JOIN (SELECT 
                            source
                            , del.account_voteAccount
                            , del.filled_block_time as call_block_time --fill in nulls with fake date
                            , del.filled_block_slot as call_block_slot
                        FROM {{ ref('staking_solana_stake_actions') }} a
                        LEFT JOIN (SELECT *
                                        , COALESCE(call_block_time, timestamp '2000-01-01 00:00:00') as filled_block_time
                                        , call_block_slot as filled_block_slot
                                    FROM {{ source ('stake_program_solana','stake_call_DelegateStake') }}
                                    ) del
                            ON del.account_stakeAccount = a.source
                            AND del.filled_block_time <= a.block_time
                        WHERE action = 'split'
                    ) as old_vote
                    ON old_vote.source = a.source --get source of split's delegated validator
                    AND old_vote.call_block_time <= a.block_time --before the split, not after
            --get newly delegated vote accounts. some will still having missing vote accounts because of multiple splits - should have some sort of recursive split detection in the future.
            LEFT JOIN {{ source ('stake_program_solana','stake_call_DelegateStake') }} del ON del.account_stakeAccount = a.destination
            WHERE a.action = 'split'
            {% if is_incremental() %}
            and {{incremental_predicate('a.block_time')}}
            {% endif %}
        )
    )

SELECT 
    distinct 
    account_stakeAccount as stake_account
    , account_voteAccount as vote_account
    , call_block_time as block_time
    , call_block_slot as block_slot
FROM all_delegates
WHERE account_voteAccount is not null --see note about split recursion above for why there are nulls