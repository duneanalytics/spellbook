{{ config(
    schema = 'tokens_solana',
    alias = 'sol_transfers',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'delete+insert',
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
        lamports as amount,
        lamports / 1e9 as amount_display,
        'So11111111111111111111111111111111111111112' as token_mint_address,
        call_outer_executing_account as outer_executing_account,
        call_inner_executing_account as inner_executing_account,
        'transfer' as action
    FROM {{ source('system_program_solana', 'system_program_call_Transfer') }}
    WHERE 1=1
    {% if is_incremental() %}
    AND {{incremental_predicate('call_block_time')}}
    {% endif %}
    {% if not is_incremental() %}
    AND call_block_time > now() - interval '30' day
    {% endif %}
)

, prices AS (
    SELECT
        contract_address,
        minute,
        price,
        decimals
    FROM {{ source('prices', 'usd_forward_fill') }}
    WHERE blockchain = 'solana'
    AND contract_address =  0x069b8857feab8184fb687f634618c035dac439dc1aeb3b5598a0f00000000001 -- SOL address
    AND minute >= TIMESTAMP '2020-10-02 00:00'
    {% if is_incremental() %}
    AND {{incremental_predicate('minute')}}
    {% endif %}
    {% if not is_incremental() %}
    AND minute > now() - interval '30' day
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
    t.amount_display,
    t.amount,
    p.price * (t.amount_display) as amount_usd,
    p.price as price_usd,
    t.action,
    t.outer_executing_account,
    t.inner_executing_account,
    t.token_version
FROM transfers t
LEFT JOIN prices p
    ON p.minute = date_trunc('minute', t.block_time)
WHERE 1=1 
{% if is_incremental() %}
AND {{incremental_predicate('t.block_time')}}
{% endif %}
