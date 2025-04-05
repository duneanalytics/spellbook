{{
  config(
        schema = 'solana_utils',
        alias = 'token_accounts_combined',
        materialized = 'view',
        post_hook='{{ expose_spells(\'["solana"]\',
                                    "sector",
                                    "solana_utils",
                                    \'["ilemi"]\') }}')
}}

-- This model combines both static and timed token accounts into one unified view
-- Static accounts (the majority) are processed efficiently with simpler logic
-- Timed accounts (with multiple owners/mints) get the full time-period processing

SELECT
    address,
    token_balance_owner,
    token_mint_address,
    valid_from,
    valid_to,
    account_type,
    'static' AS source_model
FROM {{ ref('solana_utils_token_accounts_static') }}

UNION ALL

SELECT
    address,
    token_balance_owner,
    token_mint_address,
    valid_from,
    valid_to,
    account_type,
    'timed' AS source_model
FROM {{ ref('solana_utils_token_accounts_timing') }} 