{{
  config(
        schema = 'jupiter_v6_solana',
        alias = 'aggregator_swaps',
        partition_by = ['block_month'],
        materialized='incremental',
        file_format = 'delta',
        incremental_strategy='merge',
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
        unique_key = ['block_month','amm','log_index','tx_id','output_mint','input_mint'],
        pre_hook='{{ enforce_join_distribution("PARTITIONED") }}'
    )
}}

{% set project_start_date = '2023-08-03' %}

with
    amms as (
        SELECT * FROM {{ ref('jupiter_solana_amms') }}
    )

    , jup6_logs as (
        --uses event CPI
        SELECT
            a.amm_name
            , toBase58(bytearray_substring(data,1+16,32)) as amm
            , toBase58(bytearray_substring(data,1+48,32)) as input_mint
            , CASE 
                -- Apply the 4-byte fix for 1DEX AMM after August 23rd, 2024
                -- 1DEX AMM uses the trailing 4 bytes for other msging, they are unique in that
                -- all other dexes use the full 8 bytes
                WHEN a.amm = 'DEXYosS6oEGvk8uCDayvwEZz4qEyDJRf9nFgYCaqPMTm' 
                    AND l.block_time >= TIMESTAMP '2024-08-23 00:00:00' 
                    THEN 
                    bytearray_to_bigint(bytearray_reverse(bytearray_substring(data,1+80,4)))
                -- Keep the original 8-byte parsing for all other AMMs
                ELSE 
                    bytearray_to_bigint(bytearray_reverse(bytearray_substring(data,1+80,8)))
            END as input_amount
            , toBase58(bytearray_substring(data,1+88,32)) as output_mint
            , bytearray_to_bigint(bytearray_reverse(bytearray_substring(data,1+120,8))) as output_amount
            , log_index
            , block_slot
            , block_time
            , tx_id
            , tx_signer
            , 6 as jup_version
        FROM (
            SELECT
                *
                , row_number() over (partition by tx_id order by outer_instruction_index asc, COALESCE(inner_instruction_index,0) asc) as log_index
            FROM {{ source('solana','instruction_calls') }}
            WHERE
                executing_account = 'JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4'
                AND bytearray_substring(data,1+8,8) = 0x40c6cde8260871e2 --SwapEvent https://solscan.io/account/JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4#anchorProgramIDL
                and tx_success = true
                {% if is_incremental() -%}
                AND {{ incremental_predicate('block_time') }}
                {% else -%}
                AND block_time >= TIMESTAMP '{{ project_start_date }}'
                {% endif -%}
        ) l
        JOIN amms a ON a.amm = toBase58(bytearray_substring(l.data,1+16,32)) --only include amms that we are tracking.
    )

SELECT
    l.amm
    , l.amm_name
    , case when input_mint > output_mint then tk_1.symbol || '-' || tk_2.symbol
        else tk_2.symbol || '-' || tk_1.symbol
        end as token_pair
    , tk_1.symbol as input_symbol
    , l.input_mint
    , l.input_amount
    , tk_1.decimals as input_decimals
    , l.input_amount/pow(10,p_1.decimals)*p_1.price as input_usd
    , tk_2.symbol as output_symbol
    , l.output_mint
    , l.output_amount
    , l.output_amount/pow(10,p_2.decimals)*p_2.price as output_usd
    , tk_2.decimals as output_decimals
    , l.log_index
    , l.tx_id
    , l.block_slot
    , l.block_time
    , CAST(date_trunc('month', l.block_time) as DATE) as block_month
    , l.tx_signer
    , l.jup_version
FROM jup6_logs l
--tokens
LEFT JOIN {{ source('tokens_solana','fungible') }} tk_1 ON tk_1.token_mint_address = l.input_mint
LEFT JOIN {{ source('tokens_solana','fungible') }} tk_2 ON tk_2.token_mint_address = l.output_mint
LEFT JOIN {{ source('prices','usd_forward_fill') }} p_1 ON p_1.blockchain = 'solana'
    AND date_trunc('minute', l.block_time) = p_1.minute
    AND l.input_mint = toBase58(p_1.contract_address)
    {% if is_incremental() -%}
    AND {{ incremental_predicate('p_1.minute') }}
    {% else -%}
    AND p_1.minute >= TIMESTAMP '{{ project_start_date }}'
    {% endif -%}
LEFT JOIN {{ source('prices', 'usd_forward_fill') }}  p_2 ON p_2.blockchain = 'solana'
    AND date_trunc('minute', l.block_time) = p_2.minute
    AND l.output_mint = toBase58(p_2.contract_address)
    {% if is_incremental() -%}
    AND {{ incremental_predicate('p_2.minute') }}
    {% else -%}
    AND p_2.minute >= TIMESTAMP '{{ project_start_date }}'
    {% endif -%}
WHERE l.input_mint not in ('4PfN9GDeF9yQ37qt9xCPsQ89qktp1skXfbsZ5Azk82Xi')
AND l.output_mint not in ('4PfN9GDeF9yQ37qt9xCPsQ89qktp1skXfbsZ5Azk82Xi')