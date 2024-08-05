{{ config(
    schema = 'staking_solana'
    , alias = 'validator_stake_account_epochs'
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['epoch', 'stake_account', 'vote_account']
    , post_hook='{{ expose_spells(\'["solana"]\',
                                "sector",
                                "staking",
                                \'["ilemi"]\') }}')
}}

SELECT
    b.epoch
    , b.epoch_time
    , b.epoch_start_slot
    , b.epoch_end_slot
    , b.stake_account
    , b.vote_account
    , max_by(bal.sol_balance, bal.day) as sol_balance
FROM {{ ref('staking_solana_validator_stake_account_epochs_raw')}} b
LEFT JOIN solana_utils.daily_balances bal ON bal.address = b.stake_account 
    AND bal.token_mint_address is null
    AND bal.day <= date_trunc('day', b.epoch_time)
{% if is_incremental() %}
WHERE {{incremental_predicate('b.epoch_time')}}
{% endif %}
GROUP BY 1,2,3,4,5,6