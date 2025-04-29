 {{
  config(
        schema = 'jupiter_solana',
        alias = 'aggregator_swaps',
        partition_by = ['block_month'],
        materialized='incremental',
        file_format = 'delta',
        incremental_strategy='merge',
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
        unique_key = ['log_index','tx_id','output_mint','input_mint'],
        pre_hook='{{ enforce_join_distribution("PARTITIONED") }}',
        post_hook='{{ expose_spells(\'["jupiter"]\',
                                    "project",
                                    "jupiter_solana",
                                    \'["ilemi"]\') }}')
}}

with
    amms as (
        SELECT
        *
        FROM (
            --use this api https://api.jup.ag/swap/v1/program-id-to-label
            values
                ('Perena', 'NUMERUNsFCP3kuNmWZuXtm1AaQCPj9uw6Guv2Ekoi5P'),
                ('stabble Stable Swap', 'swapNyd8XiQwJ6ianp9snpu4brUqFxadzvHebnAXjJZ'),
                ('stabble Weighted Swap', 'swapFpHZwjELNnjvThjajtiVmkz3yPQEHjLtka2fwHW'),
                ('Invariant', 'HyaB3W9q6XdA5xwpU4XnSZV94htfmbmqJXZcEbRaJutt'),
                ('Saber (Decimals)', 'DecZY86MU5Gj7kppfUCEmd4LbXXuyZH1yHaP2NTqdiZB'),
                ('Balansol', 'D3BBjqUdCYuP18fNvvMbPAZ8DpcRi4io2EsYHQawJDag'),
                ('Aldrin V2', 'CURVGoZn8zycx6FXwwevgBTB2gVvdbGTEpvMJDbgs2t4'),
                ('Phoenix', 'PhoeNiXZ8ByJGLkxNfZRnkUfjvmuYqLR89jjFHGqdXY'),
                ('StepN', 'Dooar9JkhdZ7J3LHN3A7YCuoGRUggXhQaG4kijfLGU2j'),
                ('Sanctum', 'stkitrT1Uoy18Dk1fTrgPw8W6MVzoCfYoAFT4MLsmhq'),
                ('Jupiter LO', 'jupoNjAxXgZ4rjzxzPMP4oxduvQsQtZzyknqvzYNrNu'),
                ('Orca V2', '9W959DqEETiGZocYWCQPaJ6sBmUzgfxXfqGeTEdp3aQP'),
                ('Dexlab', 'DSwpgjMvXhtGn6BsbqmacdBZyfLj6jSWf3HJpdJtmg6N'),
                ('Clone', 'C1onEW2kPetmHmwe74YC1ESx3LnFEpVau6g2pg4fHycr'),
                ('OpenBook','srmqPvymJeFKQ4zGQed1GFppgkRHL9kaELCbyksJtPX'),
                ('OpenBookV2','opnb2LAfJYbRMAHHvqjCwQxanZn7ReEHp1k81EohpZb'),
                ('Meteora', 'Eo7WjKq67rjJQSZxS6z3YkapzY3eMj6Xy8X5EQVn5UaB'),
                ('Meteora', 'LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo'),
                ('Mercurial', 'MERLuDFBMmsHnsBPZw2sDQZHvXFMwp8EdjudcU2HKky'),
                ('FluxBeam', 'FLUXubRmkEi2q6K3Y9kBPg9248ggaZVsoSFhtJHSrm1X'),
                ('Raydium CLMM', 'CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK'),
                ('Raydium CP', 'CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C'),
                ('Aldrin', 'AMM55ShdkoGRB5jVYPjWziwk8m5MpwyDgsMWHaMSQWH6'),
                ('Orca (Whirlpool)', 'whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc'),
                ('Orca V1', 'DjVE6JNiYqPL2QXyCUUh8rNjHrbz9hXHNYt99MQ59qw1'),
                ('Crema', 'CLMM9tUoggJu2wagPkkqs9eFG4BWhVBZWkP1qv3Sp7tR'),
                ('Helium Network', 'treaf4wWBBty3fHdyBpo35Mz84M8k3heKXmjmi9vFt5'),
                ('Cropper Legacy', 'CTMAxxk34HjKWxQ3QLZK1HpaLXmBveao3ESePXbiyfzh'),
                ('Saros', 'SSwapUtytfBdBn1b9NUGG6foMVPtcWgpRU32HToDUZr'),
                ('Oasis', '9tKE7Mbmj4mxDjWatikzGAtkoWosiiZX9y6J4Hfm2R8H'),
                ('Symmetry', '2KehYt3KsEQR53jYcxjbQp2d2kCp4AkuQW68atufRwSr'),
                ('Raydium amm', '675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8'),
                ('Penguin', 'PSwapMdSai8tjrEXcxFeQth87xC4rRsa4VA5mhGhXkP'),
                ('Lifinity v1', 'EewxydAPCCVuNEyrVN68PuSYdQ7wKn27V9Gjeoi8dy3S'),
                ('Saber swap', 'SSwpkEEcbUqx4vtoEByFjSkhKdCT862DNVb52nZg1UZ'),
                ('Marinade', 'MarBmsSgKXdrN1egZf5sqe1TMai9K1rChYNDJgjq7aD'),
                ('Bonkswap', 'BSwp6bEBihVLdqJRKGgzjcGLHkcTuzmSo1TQkHepzH8p'),
                ('Serum v3', '9xQeWvG816bUx9EPjHmaT23yvVM2ZWbrrpZb9PusVFin'),
                ('Lifinity v2', '2wT8Yq49kHgDzXuPxZSaeLaH1qbmGXtEyPy64bL7aD3c'),
                ('GooseFX SSL v2', 'GFXsSL5sSaDfNFQUYsHekbWBW1TsFdjDYzACh62tEHxn'),
                ('Cropper', 'H8W3ctz92svYg6mkn1UtGfu2aQr2fnUFHM1RhScEtQDt'),
                ('Sanctum Infinity', '5ocnV1qiCgaQR8Jb8xWnVbApfaygJ8tNoZfgPwsgx9kx'),
                ('Token Swap', 'SwaPpA9LAaLfeLi3a68M4DjnLqgtticKg6CnyNwgAC8'),
                ('Jupiter Perps', 'PERPHjGBqRHArX4DySjwM6UJHiR3sWAatqfdBS2qQJu'),
                ('Guacswap', 'Gswppe6ERWKpUTXvRPfXdzHhiCyJvLadVvXGfdpBqcE1'),
                ('SolFi', 'SoLFiHG9TfgtdUXUjWAxi3LtvYuFyDLVhBWxdMZxyCe'),
                ('Pump.fun', '6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P'),
                ('1DEX', 'DEXYosS6oEGvk8uCDayvwEZz4qEyDJRf9nFgYCaqPMTm'),
                ('Obric V2', 'obriQD1zbpyLz95G5n7nJe6a4DPjpFwa5XYPoNm113y'),
                ('Mooshot', 'MoonCVVNZFSYkqNXP6bxHLPL6QQJiMagDL3qcqUQTrG'),
                ('Fox', 'HyhpEq587ANShDdbx1mP4dTmDZC44CXWft29oYQXDb53'),
                ('Solayer', 'endoLNCKTqDn8gSVnN2hDdpgACUPWHZTwoYnnMybpAT'),
                ('Token Mill', 'JoeaRXgtME3jAoz5WuFXGEndfv4NPH9nBxsLq44hk9J'),
                ('Daos.fun', '5jnapfrAN47UYkLkEf7HnprPPBCQLvkYWGZDeKkaP5hv'),
                ('ZeroFi', 'ZERor4xhbUycZ6gb9ntrhqscUcZmAbQDjEAtCf4hbZY')
            ) as v(amm_name, amm)
    )


    , jup_messages_logs as (
        --SwapEvent through log messages
        with
            hex_data as (
                SELECT
                    from_base64(split(l.logs, ' ')[3]) as hex_data
                    -- , split(logs, ' ')[3] as base64_data
                    , l.log_index_raw
                    , row_number() over (partition by t.id order by l.log_index_raw asc) as log_index
                    , t.id as tx_id
                    , t.block_slot
                    , t.block_time
                    , t.signer as tx_signer
                    , case when contains(t.account_keys, 'JUP5pEAZeHdHrLxh5UCwAbpjGwYKKoquCpda2hfP4u8') then 5
                        when contains(t.account_keys, 'JUP4Fb2cqiRUcaTHdrPC8h2gNsA2ETXiPDD33WcGuJB') then 4
                        end as jup_version
                FROM {{ source('solana','transactions') }} t
                LEFT JOIN unnest(log_messages) WITH ORDINALITY as l(logs, log_index_raw) ON true
                WHERE success = True
                AND any_match(account_keys, x->x IN ('JUP4Fb2cqiRUcaTHdrPC8h2gNsA2ETXiPDD33WcGuJB','JUP5pEAZeHdHrLxh5UCwAbpjGwYKKoquCpda2hfP4u8'))
                AND REGEXP_LIKE(l.logs, 'Program data:.*|Program log.*')
                AND try(from_base64(split(l.logs, ' ')[3])) is not null --valid hex
                AND bytearray_substring(from_base64(split(l.logs, ' ')[3]), 1, 8) IN (0x516ce3becdd00ac4, 0x40c6cde8260871e2) --v4, v5 discriminator
                {% if is_incremental() %}
                AND {{ incremental_predicate('block_time') }}
                {% endif %}
                -- and block_time >= now() - interval '7' day --shorten CI
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

    , jup6_logs as (
        --uses event CPI
        SELECT
            a.amm_name
            , toBase58(bytearray_substring(data,1+16,32)) as amm
            , toBase58(bytearray_substring(data,1+48,32)) as input_mint
            , bytearray_to_bigint(bytearray_reverse(bytearray_substring(data,1+80,8))) as input_amount
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
            WHERE executing_account = 'JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4'
            AND bytearray_substring(data,1+8,8) = 0x40c6cde8260871e2 --SwapEvent https://solscan.io/account/JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4#anchorProgramIDL
            and tx_success = true
            {% if is_incremental() %}
            AND {{ incremental_predicate('block_time') }}
            {% endif %}
            -- and block_time >= now() - interval '7' day --shorten CI
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
FROM (
    SELECT * FROM jup_messages_logs
    WHERE input_amount > 0 and output_amount > 0
    UNION ALL
    SELECT * FROM jup6_logs
) l
--tokens
LEFT JOIN {{ ref('tokens_solana_fungible') }} tk_1 ON tk_1.token_mint_address = l.input_mint
LEFT JOIN {{ ref('tokens_solana_fungible') }} tk_2 ON tk_2.token_mint_address = l.output_mint
LEFT JOIN {{ source('prices','usd_forward_fill') }} p_1 ON p_1.blockchain = 'solana'
    AND date_trunc('minute', l.block_time) = p_1.minute
    AND l.input_mint = toBase58(p_1.contract_address)
    {% if is_incremental() %}
    AND {{ incremental_predicate('p_1.minute') }}
    {% endif %}
LEFT JOIN {{ source('prices', 'usd_forward_fill') }}  p_2 ON p_2.blockchain = 'solana'
    AND date_trunc('minute', l.block_time) = p_2.minute
    AND l.output_mint = toBase58(p_2.contract_address)
    {% if is_incremental() %}
    AND {{ incremental_predicate('p_2.minute') }}
    {% endif %}
WHERE l.input_mint not in ('4PfN9GDeF9yQ37qt9xCPsQ89qktp1skXfbsZ5Azk82Xi')
AND l.output_mint not in ('4PfN9GDeF9yQ37qt9xCPsQ89qktp1skXfbsZ5Azk82Xi')