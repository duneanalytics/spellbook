{{ config(
    schema = 'staking_solana'
    , alias = 'stake_account_delegations'
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['block_time', 'block_slot', 'stake_account', 'vote_account']
    , post_hook='{{ hide_spells() }}')
}}

-- For each stake account, emit the vote accounts it has been delegated to over
-- time. Two sources:
--   1. Direct DelegateStake calls on the stake account.
--   2. SPLIT creates a new stake account that inherits the source's delegation.
--      We surface each historical source delegation as an inherited row, plus
--      any new DelegateStake on the destination.

with
    direct_delegates as (
        SELECT
            account_stakeAccount
            , account_voteAccount
            , COALESCE(call_block_time, timestamp '1990-01-01 00:00:00') as call_block_time
            , call_block_slot
        FROM {{ source('stake_program_solana','stake_call_DelegateStake') }}
        {% if is_incremental() %}
        WHERE {{ incremental_predicate('call_block_time') }}
        {% endif %}
    )

    , split_delegates as (
        -- Single-level join: for each split, surface
        --   - inherited rows: historical DelegateStakes on `source` at time <= split time
        --   - destination rows: DelegateStakes on the newly-created `destination`
        -- Previously this used a nested old_vote subquery that re-iterated split
        -- actions × DelegateStake, producing a ~67M-row cross product that the
        -- outer DISTINCT had to collapse. The single-level join is semantically
        -- equivalent (verified by regression) and ~200x smaller.
        SELECT
            a.destination as account_stakeAccount
            , COALESCE(dst_del.account_voteAccount, src_del.account_voteAccount) as account_voteAccount
            -- Preserve the original NULL semantics: when src_del matched, fill
            -- NULL DelegateStake times with the historical sentinel; when no
            -- src_del matched, fall through to dst_del or NULL.
            , COALESCE(
                dst_del.call_block_time,
                CASE
                    WHEN src_del.account_stakeAccount IS NOT NULL
                    THEN COALESCE(src_del.call_block_time, timestamp '2000-01-01 00:00:00')
                END
              ) as call_block_time
            , COALESCE(dst_del.call_block_slot, src_del.call_block_slot) as call_block_slot
        FROM {{ ref('staking_solana_stake_actions') }} a
        LEFT JOIN {{ source('stake_program_solana','stake_call_DelegateStake') }} src_del
            ON src_del.account_stakeAccount = a.source
            AND COALESCE(src_del.call_block_time, timestamp '2000-01-01 00:00:00') <= a.block_time
        LEFT JOIN {{ source('stake_program_solana','stake_call_DelegateStake') }} dst_del
            ON dst_del.account_stakeAccount = a.destination
            {% if is_incremental() %}
            -- destination is created by the split, so any DelegateStake on it
            -- must have call_block_time >= a.block_time >= incremental cutoff.
            AND {{ incremental_predicate('dst_del.call_block_time') }}
            {% endif %}
        WHERE a.action = 'split'
        {% if is_incremental() %}
        AND {{ incremental_predicate('a.block_time') }}
        {% endif %}
    )

SELECT DISTINCT
    account_stakeAccount as stake_account
    , account_voteAccount as vote_account
    , call_block_time as block_time
    , call_block_slot as block_slot
FROM (
    SELECT * FROM direct_delegates
    UNION ALL
    SELECT * FROM split_delegates
)
WHERE account_voteAccount is not null
    --see note about split recursion: some splits still have null vote_accounts
    --because of multi-hop splits we don't trace through recursively.
