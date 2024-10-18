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
                                    "tokens_solana",
                                    \'["ilemi", "0xBoxer"]\') }}')
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
WHERE 1=1
{% if is_incremental() %}
AND {{incremental_predicate('block_time')}}
{% endif %}
{% if not is_incremental() %}
AND block_time > now() - interval '5' hour
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
WHERE 1=1
{% if is_incremental() %}
AND {{incremental_predicate('block_time')}}
{% endif %}
{% if not is_incremental() %}
AND block_time > now() - interval '5' hour
{% endif %}
UNION ALL
    SELECT
    block_time
    , block_slot
    , action
    , amount
    , cast (null as double) as fee
    , from_owner
    , to_owner
    , token_version
    , tx_signer
    , tx_id
    , outer_instruction_index
    , inner_instruction_index
    , outer_executing_account
FROM {{ ref('tokens_solana_sol_transfers') }}
WHERE 1=1
{% if is_incremental() %}
AND {{incremental_predicate('block_time')}}
{% endif %}
{% if not is_incremental() %}
AND block_time > now() - interval '5' hour
{% endif %}
)

,final_transfers as (
SELECT
    block_time
    , cast (date_trunc('day', block_time) as date) as block_date
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
LEFT JOIN {{ ref('solana_utils_token_accounts') }} tk_s ON tk_s.address = tr.from_token_account and token_version <> 'native'
LEFT JOIN {{ ref('solana_utils_token_accounts') }} tk_d ON tk_d.address = tr.to_token_account and token_version <> 'native'
WHERE 1=1
{% if is_incremental() %}
AND {{incremental_predicate('block_time')}}
{% endif %}
{% if not is_incremental() %}
AND block_time > now() - interval '5' hour
{% endif %}
)

SELECT 
    block_time
    , block_date
    , block_slot
    , action
    , amount
    , p.price * amount as amount_usd
    , fee
    , token_mint_address
    , from_owner
    , to_owner
    , from_token_account
    , to_token_account
    , token_version
    , tx_signer
    , tx_id
    , outer_instruction_index
    , inner_instruction_index
    , outer_executing_account
 FROM final_transfers
 LEFT JOIN {{ source('prices', 'usd_forward_fill') }} p
    ON p.blockchain = 'solana'
    AND to_base58(p.contract_address) = token_mint_address
    AND p.minute = date_trunc('minute', block_time)
    {% if is_incremental() %}
    AND {{incremental_predicate('p.minute')}}
    {% endif %}
    {% if not is_incremental() %}
    AND p.minute > now() - interval '5' hour
    {% endif %}
WHERE 1=1
{% if is_incremental() %}
AND {{incremental_predicate('block_time')}}
{% endif %}
{% if not is_incremental() %}
AND block_time > now() - interval '5' hour
{% endif %}
