{{
  config(
    schema = 'jupiter_solana',
    alias = 'dca_volume',
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['tx_id', 'outer_instruction_index', 'inner_instruction_index'],
    post_hook='{{ expose_spells(\'["solana"]\',
                                      "project",
                                      "jupiter_solana",
                                      \'["ilemi"]\') }}')
}}

SELECT
    io.call_block_time as block_time
    , date_trunc('day',io.call_block_time) block_date
    , io.call_tx_id as tx_id
    , io.call_outer_instruction_index as outer_instruction_index
    , COALESCE(io.call_inner_instruction_index,0) as inner_instruction_index
    , io.account_inputMint as input_mint
    , COALESCE(tr_in_sol.lamports, tr_in.amount) as in_amount --input/initiate does not contain the swapped amount, we need to grab from the transfer.
    , tr_in.amount/pow(10,p_in.decimals)*p_in.price as in_amount_usd
    , io.account_outputMint as output_mint
    , io.repayAmount as out_amount
    , io.repayAmount/pow(10,p_out.decimals)*p_out.price as out_amount_usd
    , dca_t.account_user
    , io.account_dca
FROM {{ source('dca_solana','dca_call_fulfillFlashFill') }} io
LEFT JOIN {{ source('dca_solana','dca_call_transfer') }} dca_t ON dca_t.call_tx_id = io.call_tx_id AND dca_t.call_block_time = io.call_block_time
LEFT JOIN {{ ref('tokens_solana_transfers') }} tr_in ON tr_in.outer_instruction_index = io.call_outer_instruction_index - 2 --initiate, route, fill, transfer (we want initiate)
    AND tr_in.inner_instruction_index = COALESCE(io.call_inner_instruction_index, 0) + 1
    AND tr_in.tx_id = io.call_tx_id
    AND tr_in.block_slot = io.call_block_slot
    AND tr_in.block_time = io.call_block_time
LEFT JOIN {{ source('system_program_solana','system_program_call_Transfer') }} tr_in_sol ON tr_in_sol.call_outer_instruction_index = io.call_outer_instruction_index - 2 --same comment as above
    AND tr_in_sol.call_inner_instruction_index = COALESCE(io.call_inner_instruction_index, 0) + 1
    AND tr_in_sol.call_tx_id = io.call_tx_id
    AND tr_in_sol.call_block_slot = io.call_block_slot
    AND tr_in_sol.call_block_time = io.call_block_time
    AND tr_in_sol.lamports/1e9 > 1 --filter out small transfers to make table smaller 
LEFT JOIN {{ source('prices', 'usd') }} p_in ON p_in.blockchain = 'solana' 
    and toBase58(p_in.contract_address) = io.account_inputMint
    and p_in.minute = date_trunc('minute',io.call_block_time)
LEFT JOIN {{ source('prices', 'usd') }} p_out ON p_out.blockchain = 'solana' 
    and toBase58(p_out.contract_address) = io.account_outputMint
    and p_out.minute = date_trunc('minute',io.call_block_time)
{% if is_incremental() %}
WHERE io.call_block_time >= date_trunc('day', now() - interval '7' day)
{% endif %}
{% if not is_incremental() %}
WHERE io.call_block_time >= date_trunc('day', now() - interval '1' day) --choose a smaller date to test with
{% endif %}