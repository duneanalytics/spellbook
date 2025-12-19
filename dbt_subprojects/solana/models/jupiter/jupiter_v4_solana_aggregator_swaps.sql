{{
  config(
        schema = 'jupiter_v4_solana',
        alias = 'aggregator_swaps',
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy='merge',
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
        unique_key = ['block_month','amm','log_index','tx_id','output_mint','input_mint'],
        pre_hook = '{{ enforce_join_distribution("PARTITIONED") }}'
    )
}}

{% set project_start_date = '2022-09-22' %}

with
    amms as (
        SELECT * FROM {{ ref('jupiter_solana_amms') }}
    )

    , jup_messages_logs as (
        --SwapEvent through log messages
        with
            hex_data as (
                SELECT
                    from_base64(split(l.logs, ' ')[3]) as hex_data
                    , l.log_index_raw
                    , row_number() over (partition by t.id order by l.log_index_raw asc) as log_index
                    , t.id as tx_id
                    , t.block_slot
                    , t.block_time
                    , t.signer as tx_signer
                    , 4 as jup_version
                FROM {{ source('solana','transactions') }} t
                LEFT JOIN unnest(log_messages) WITH ORDINALITY as l(logs, log_index_raw) ON true
                WHERE
                    success = True
                    AND any_match(account_keys, x->x IN ('JUP4Fb2cqiRUcaTHdrPC8h2gNsA2ETXiPDD33WcGuJB'))
                    -- avoid double counting when both v4 and v5 programs are present; v5 model takes precedence to write over v4 swaps
                    AND NOT any_match(account_keys, x->x IN ('JUP5pEAZeHdHrLxh5UCwAbpjGwYKKoquCpda2hfP4u8'))
                    AND REGEXP_LIKE(l.logs, 'Program data:.*|Program log.*')
                    AND try(from_base64(split(l.logs, ' ')[3])) is not null --valid hex
                    AND bytearray_substring(from_base64(split(l.logs, ' ')[3]), 1, 8) IN (0x516ce3becdd00ac4, 0x40c6cde8260871e2) --v4, v5 discriminator
                    {% if is_incremental() -%}
                    AND {{ incremental_predicate('block_time') }}
                    {% else -%}
                    AND block_time >= TIMESTAMP '{{ project_start_date }}'
                    {% endif -%}
        )

        SELECT
            a.amm_name
            , toBase58(bytearray_substring(hex_data,1+8,32)) as amm
            , toBase58(bytearray_substring(hex_data,1+40,32)) as input_mint
            , bytearray_to_bigint(bytearray_reverse(bytearray_substring(hex_data,1+72,8))) as input_amount
            , toBase58(bytearray_substring(hex_data,1+80,32)) as output_mint
            , bytearray_to_bigint(bytearray_reverse(bytearray_substring(hex_data,1+112,8))) as output_amount
            , log_index
            , block_slot
            , block_time
            , tx_id
            , tx_signer
            , jup_version
        FROM hex_data
        JOIN amms a ON a.amm = toBase58(bytearray_substring(hex_data,1+8,32)) --only include amms that we are tracking.
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
FROM jup_messages_logs l
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
WHERE l.input_amount > 0
AND l.output_amount > 0
AND l.input_mint not in ('4PfN9GDeF9yQ37qt9xCPsQ89qktp1skXfbsZ5Azk82Xi')
AND l.output_mint not in ('4PfN9GDeF9yQ37qt9xCPsQ89qktp1skXfbsZ5Azk82Xi')