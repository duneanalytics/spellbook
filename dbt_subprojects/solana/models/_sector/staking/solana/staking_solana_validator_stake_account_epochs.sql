{{ config(
    schema = 'staking_solana'
    , alias = 'validator_stake_account_epochs'
    , materialized = 'table'
    , post_hook='{{ expose_spells(\'["solana"]\',
                                "sector",
                                "staking",
                                \'["ilemi"]\') }}')
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
GROUP BY 1,2,3,4,5,6
