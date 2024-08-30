 {{
  config(
        schema = 'tokens_solana',
        alias = 'fees_history',
        tags = ['prod_exclude'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.fee_time')],
        unique_key = ['account_mint','fee_time'],
        post_hook='{{ expose_spells(\'["solana"]\',
                                    "sector",
                                    "tokens_solana",
                                    \'["ilemi"]\') }}')
}}
--we need the fee basis points and maximum fee for token2022 transfers because the fee amount is not emitted in transferChecked
SELECT
call_account_arguments[1] as account_mint
, try(bytearray_to_uint256(bytearray_reverse(bytearray_substring(call_data,
            1+1+1+1+1+case when bytearray_substring(call_data,1+1+1,1) = 0x01 and bytearray_substring(call_data,1+1+1+32+1,1) = 0x01
                        then 64
                        when bytearray_substring(call_data,1+1+1,1) = 0x01 and bytearray_substring(call_data,1+1+1+32+1,1) = 0x00
                        then 32
                        when bytearray_substring(call_data,1+1+1,1) = 0x00 and bytearray_substring(call_data,1+1+1+1,1) = 0x01
                        then 32
                        when bytearray_substring(call_data,1+1+1,1) = 0x00 and bytearray_substring(call_data,1+1+1+1,1) = 0x00
                        then 0
                        end --variations of COPTION enums for first two arguments
            ,2)))) as fee_basis
, try(bytearray_to_uint256(bytearray_reverse(bytearray_substring(call_data,
            1+1+1+1+1+case when bytearray_substring(call_data,1+1+1,1) = 0x01 and bytearray_substring(call_data,1+1+1+32+1,1) = 0x01
                        then 64
                        when bytearray_substring(call_data,1+1+1,1) = 0x01 and bytearray_substring(call_data,1+1+1+32+1,1) = 0x00
                        then 32
                        when bytearray_substring(call_data,1+1+1,1) = 0x00 and bytearray_substring(call_data,1+1+1+1,1) = 0x01
                        then 32
                        when bytearray_substring(call_data,1+1+1,1) = 0x00 and bytearray_substring(call_data,1+1+1+1,1) = 0x00
                        then 0
                        end
                +2
            ,16)))) as fee_maximum
, call_block_time as fee_time
FROM {{ source('spl_token_2022_solana','spl_token_2022_call_transferFeeExtension') }}
WHERE bytearray_substring(call_data,1+1,1) = 0x00 --https://github.com/solana-labs/solana-program-library/blob/8f50c6fabc6ec87ada229e923030381f573e0aed/token/program-2022/src/extension/transfer_fee/instruction.rs#L38
    AND call_account_arguments[1] != '9Fy4NYzaUTA4qayuhSMPjUq5YMAiBR7BMmAXCiUHMRdt' --returns duplicates
{% if is_incremental() %}
AND {{incremental_predicate('call_block_time')}}
{% endif %}

UNION ALL
SELECT
call_account_arguments[1] as account_mint
, try(bytearray_to_uint256(bytearray_reverse(bytearray_substring(call_data,
            1+1+1,2)))) as fee_basis
, try(bytearray_to_uint256(bytearray_reverse(bytearray_substring(call_data,
            1+1+1+2,16)))) as fee_maximum
, call_block_time as fee_time
FROM {{ source('spl_token_2022_solana','spl_token_2022_call_transferFeeExtension') }}
WHERE bytearray_substring(call_data,1+1,1) = 0x05 --https://github.com/solana-labs/solana-program-library/blob/8f50c6fabc6ec87ada229e923030381f573e0aed/token/program-2022/src/extension/transfer_fee/instruction.rs#L147
    AND call_account_arguments[1] != '9Fy4NYzaUTA4qayuhSMPjUq5YMAiBR7BMmAXCiUHMRdt' --returns duplicates
{% if is_incremental() %}
AND {{incremental_predicate('call_block_time')}}
{% endif %}
