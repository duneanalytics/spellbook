{{ config(
    schema = 'staking_solana'
    , alias = 'stake_account_delegations'
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['block_time', 'block_slot', 'stake_account', 'vote_account']
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
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
                b.account_stakeAccount
                , del.account_voteAccount
                , COALESCE(del.call_block_time, timestamp '1990-01-01 00:00:00') as call_block_time --fill in nulls, at a fake earlier date than split thing below
                , del.call_block_slot
            FROM (
                SELECT account_stakeAccount FROM {{ source ('stake_program_solana','stake_call_Initialize') }}
                UNION ALL 
                SELECT account_stakeAccount FROM {{ source ('stake_program_solana','stake_call_InitializeChecked') }}
                UNION ALL
                SELECT 
                    case when action = 'withdraw' then source --if withdraw, then stake account is the withdraw account.
                        else destination --if merge or split, then both are stake accounts.
                        end as account_stakeAccount
                FROM {{ ref( 'staking_solana_stake_actions') }}

                UNION ALL 

                SELECT 
                    account_stakeAccount
                FROM unnest(
                    array[
                    --these three are part of a set of accounts that somehow have no "create" or "init" tx.
                    '2YPQaJk2x74yaW3jQuDzkcpgo3Vz99BpEtAcUbDRc5Pb'
                    ,'HzpGLJ1v1vf1ptr79iJN8zCjf4eUMDs7VQNibNAsZeVL'
                    ,'AneKQ22L1ymJXQ46zLL5tuQn2s5uFcvH3dXw44uVhZdJ'
                    --deep splits 
                    ,'LzrsqaJ6beDX1GGK4m4YobJ2Vpw3HVCD4j1d2thDHF9']
                    ) as a(account_stakeAccount)
            ) b
            LEFT JOIN {{ source ('stake_program_solana','stake_call_DelegateStake') }} del ON del.account_stakeAccount = b.account_stakeAccount

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
                        FROM {{ ref('staking_solana.stake_actions') }} a
                        LEFT JOIN (SELECT *
                                        , COALESCE(call_block_time, timestamp '2000-01-01 00:00:00') as filled_block_time
                                        , call_block_slot as filled_block_slot
                                    FROM stake_program_solana.stake_call_DelegateStake) del
                            ON del.account_stakeAccount = a.source
                            AND del.filled_block_time <= a.block_time
                        WHERE action = 'split'
                    ) as old_vote
                    ON old_vote.source = a.source --get source of split's delegated validator
                    AND old_vote.call_block_time <= a.block_time --before the split, not after
            --get newly delegated vote accounts
            LEFT JOIN {{ source ('stake_program_solana','stake_call_DelegateStake') }} del ON del.account_stakeAccount = a.destination
            --select only split accounts to do these nasty joins on
            WHERE a.action = 'split'
        )
    )

SELECT 
    account_stakeAccount as stake_account_raw 
    , account_voteAccount as vote_account_raw
    , call_block_time
    , call_block_slot
    , row_number() over (partition by account_stakeAccount order by call_block_time desc) as latest
FROM all_delegates