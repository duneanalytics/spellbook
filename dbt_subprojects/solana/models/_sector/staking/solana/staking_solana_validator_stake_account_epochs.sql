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

SELECT
    b.epoch
    , b.epoch_time
    , b.epoch_start_slot
    , b.epoch_next_start_slot
    , b.stake_account
    , b.vote_account
    , max_by(bal.sol_balance, bal.day) as sol_balance
FROM {{ ref('staking_solana_validator_stake_account_epochs_raw')}} b
LEFT JOIN {{ ref('solana_utils_daily_balances')}} bal ON bal.address = b.stake_account
    AND bal.token_mint_address is null
    AND bal.day <= date_trunc('day', b.epoch_time)
{% if is_incremental() %}
-- Match upstream raw model lookback so we re-emit the in-flight epoch and
-- pick up any updates to recently re-processed (epoch, stake_account) rows.
WHERE b.epoch_time >= now() - interval '10' day
{% endif %}
GROUP BY 1,2,3,4,5,6
