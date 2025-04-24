{{ config(
    schema = 'tokens_solana'
    , alias = 'token22_spl_transfers'
    , partition_by = ['block_date']
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['block_date', 'unique_instruction_key']
) }}

with base_transfers as (
    select
        block_date
        , block_time
        , block_slot
        , tx_id
        , tx_index
        , inner_instruction_index
        , outer_instruction_index
        , tx_signer
        , amount
        , outer_executing_account
        , inner_executing_account
        , from_token_account_prefix
        , from_token_account
        , to_token_account_prefix
        , to_token_account
        , token_version
        , unique_instruction_key
    from
        {{ ref('tokens_solana_base_token22_spl_transfers') }}
    {% if is_incremental() -%}
    where
        {{ incremental_predicate('block_time') }}
    {% endif -%}
)
, prices AS (
    SELECT
        contract_address
        , minute
        , price
        , decimals
        , symbol
    FROM 
        {{ source('prices', 'usd_forward_fill') }}
    WHERE 
        blockchain = 'solana'
        AND minute >= TIMESTAMP '2020-10-02 00:00' --solana start date
        {% if is_incremental() or true -%}
        AND {{incremental_predicate('minute')}}
        {% else -%}
        AND minute >= {{start_date}}
        AND minute < {{end_date}}
        {% endif -%}
)
select
    t.block_date
    , t.block_time
    , t.block_slot
    , t.action
    , t.amount
    , CASE 
        WHEN tk_f.decimals is null THEN null
        WHEN tk_f.decimals = 0 THEN t.amount
        ELSE t.amount / power(10, tk_f.decimals)
      END as amount_display
    , CASE 
        WHEN tk_f.decimals is null THEN null
        WHEN tk_f.decimals = 0 THEN p.price * t.amount
        ELSE p.price * t.amount / power(10, tk_f.decimals)
      END as amount_usd
    , p.price as price_usd
    , COALESCE(tk_s.token_mint_address, tk_d.token_mint_address) as token_mint_address
    , tk_f.symbol
    , tk_s.token_balance_owner as from_owner
    , tk_d.token_balance_owner as to_owner
    , t.from_token_account
    , t.to_token_account
    , t.token_version
    , t.tx_signer
    , t.tx_id
    , t.tx_index
    , t.outer_instruction_index
    , t.inner_instruction_index
    , t.outer_executing_account
    , t.unique_instruction_key
from
    base_transfers as t
LEFT JOIN 
      {{ ref('solana_utils_token_accounts_state_history') }} tk_s
      ON tr.from_token_account_prefix = tk_s.address_prefix
      AND tr.from_token_account = tk_s.address
      AND tr.unique_instruction_key >= tk_s.valid_from_unique_instruction_key
      AND tr.unique_instruction_key < tk_s.valid_to_unique_instruction_key
LEFT JOIN 
      {{ ref('solana_utils_token_accounts_state_history') }} tk_d 
      ON tr.to_token_account_prefix = tk_d.address_prefix
      AND tr.to_token_account = tk_d.address
      AND tr.unique_instruction_key >= tk_d.valid_from_unique_instruction_key
      AND tr.unique_instruction_key < tk_d.valid_to_unique_instruction_key
LEFT JOIN 
      {{ ref('solana_utils_token_address_mapping') }} tk_m
      ON tk_m.base58_address = COALESCE(tk_s.token_mint_address, tk_d.token_mint_address)
LEFT JOIN 
      {{ ref('tokens_solana_fungible') }} tk_f
      ON COALESCE(tk_s.token_mint_address, tk_d.token_mint_address) = tk_f.token_mint_address
LEFT JOIN prices p
    ON p.contract_address = tk_m.binary_address
    AND p.minute = date_trunc('minute', tr.call_block_time)