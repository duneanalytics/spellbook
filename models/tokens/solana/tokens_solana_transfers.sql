 {{
  config(
        schema = 'tokens_solana',
        alias = 'transfers',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        partition_by = ['block_date'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
        unique_key = ['tx_id','outer_instruction_index','inner_instruction_index', 'block_slot'],
        post_hook='{{ expose_spells(\'["solana"]\',
                                    "sector",
                                    "tokens",
                                    \'["ilemi"]\') }}')
}}


WITH
base as (
    SELECT
    block_time
    , block_slot
    , action
    , amount
    , fee
    , from_token_account
    , to_token_account
    , token_version
    , tx_signer
    , tx_id
    , outer_instruction_index
    , inner_instruction_index
    , outer_executing_account
FROM {{ ref('tokens_solana_spl_transfers') }}
{% if is_incremental() %}
WHERE {{incremental_predicate('block_time')}}
{% endif %}
UNION ALL
    SELECT
    block_time
    , block_slot
    , action
    , amount
    , fee
    , from_token_account
    , to_token_account
    , token_version
    , tx_signer
    , tx_id
    , outer_instruction_index
    , inner_instruction_index
    , outer_executing_account
FROM {{ ref('tokens_solana_token22_spl_transfers') }}
{% if is_incremental() %}
WHERE {{incremental_predicate('block_time')}}
{% endif %}
)

SELECT
    call_block_time as block_time
    , cast (date_trunc('day', call_block_time) as date) as block_date
    , block_slot
    , action
    , amount
    , fee
    , COALESCE(tk_s.token_mint_address, tk_d.token_mint_address) as token_mint_address
    , tk_s.token_balance_owner as from_owner
    , tk_d.token_balance_owner as to_owner
    , from_token_account
    , to_token_account
    , token_version
    , tx_signer
    , tx_id
    , outer_instruction_index
    , inner_instruction_index
    , outer_executing_account
FROM base tr
--get token and accounts
INNER JOIN {{ ref('solana_utils_token_accounts') }} tk_s ON tk_s.address = tr.account_source
INNER JOIN {{ ref('solana_utils_token_accounts') }} tk_d ON tk_d.address = tr.account_destination
WHERE 1=1
-- AND call_block_time > now() - interval '90' day --for faster CI testing