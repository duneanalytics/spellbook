{{ config(
    schema = 'tokens_solana',
    alias = 'sol_transfers',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['tx_id', 'tx_index', 'inner_instruction_index', 'outer_instruction_index'],
    post_hook='{{ expose_spells(\'["solana"]\',
                                "sector",
                                "tokens",
                                \'["0xBoxer"]\') }}'
) }}

WITH transfers AS (
    SELECT
        'solana' as blockchain,
        call_block_time as block_time,
        date_trunc('day', call_block_time) AS block_date,
        date_trunc('month', call_block_time) AS block_month,
        call_block_slot as block_slot,
        call_tx_id as tx_id,
        call_tx_index as tx_index,
        call_inner_instruction_index as inner_instruction_index,
        call_outer_instruction_index as outer_instruction_index,
        call_tx_signer as tx_signer,
        call_account_arguments[1] AS from_owner,
        call_account_arguments[2] AS to_owner,
        'native' as token_version,
        'SOL' as symbol,
        lamports as amount_raw,
        lamports / 1e9 as amount,
        'So11111111111111111111111111111111111111112' as token_mint_address,
        call_outer_executing_account as outer_executing_account,
        call_inner_executing_account as inner_executing_account,
        'transfer' as action
    FROM {{ source('system_program_solana', 'system_program_call_Transfer') }}
    {% if is_incremental() %}
    WHERE {{incremental_predicate('call_block_time')}}
    {% endif %}
    {% if not is_incremental() %}
    WHERE call_block_time > now() - interval '5' hour
    {% endif %}
)

SELECT
    t.blockchain,
    t.block_month,
    t.block_date,
    t.block_time,
    t.block_slot,
    t.tx_id,
    t.tx_index,
    t.inner_instruction_index,
    t.outer_instruction_index,
    t.tx_signer,
    t.from_owner,
    t.to_owner,
    t.token_mint_address,
    t.symbol,
    t.amount_raw,
    t.amount,
    t.action,
    t.outer_executing_account,
    t.inner_executing_account,
    t.token_version
FROM transfers t
where 1=1 
{% if is_incremental() %}
AND {{incremental_predicate('t.block_time')}}
{% endif %}
{% if not is_incremental() %}
AND t.block_time > now() - interval '5' hour
{% endif %}