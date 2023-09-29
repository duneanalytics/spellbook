{{
    config(
        schema = 'magiceden_solana'
        , tags = ['dunesql']
        , alias = alias('trades')
        ,materialized = 'incremental'
        ,file_format = 'delta'
        ,incremental_strategy = 'merge'
        ,unique_key = ['project','trade_category','amount_raw','outer_instruction_index','inner_instruction_index','account_metadata','tx_id']
        ,post_hook='{{ expose_spells(\'["solana"]\',
                                    "project",
                                    "magiceden",
                                    \'["ilemi"]\') }}'
    )
}}

with 
    royalty_logs as (
        with nested_logs as (
            SELECT 
                distinct 
                call_tx_id
                , call_block_slot
                , call_outer_instruction_index
                , call_inner_instruction_index
                , cast(json_extract_scalar(json_parse(split(logs, ' ')[3]), '$.royalty') as double) as royalty
                , logs
            FROM (
                SELECT
                    call_tx_id
                    , call_block_slot
                    , call_outer_instruction_index
                    , call_inner_instruction_index
                    , call_log_messages
                FROM {{ source('magic_eden_solana','m2_call_mip1ExecuteSaleV2') }}
                UNION ALL 
                SELECT
                    call_tx_id
                    , call_block_slot
                    , call_outer_instruction_index
                    , call_inner_instruction_index
                    , call_log_messages
                FROM {{ source('magic_eden_solana','m2_call_executeSaleV2') }}
                UNION ALL 
                SELECT
                    call_tx_id
                    , call_block_slot
                    , call_outer_instruction_index
                    , call_inner_instruction_index
                    , call_log_messages
                FROM {{ source('magic_eden_solana','m2_call_ocpExecuteSaleV2') }}
            ) LEFT JOIN unnest(call_log_messages) as log_messages(logs) ON True
            WHERE logs LIKE '%Program log:%royalty%price%seller_expiry%' --must log these fields. hopefully no other programs out there log them hahaha
            AND try(json_parse(split(logs, ' ')[3])) is not null --valid hex
            -- --QA
            -- AND call_tx_id = '4RrNYCNkJCLjvj4unGgo6owJeEg5qxWZJLYnfyiJgFVypashPaJFVgQRivEvcxeEt3xrQUiUM9bm7VP76XtPenxw'
            -- AND call_block_slot = 219668172
        )
        
        SELECT
        *
        , row_number() over (partition by call_tx_id order by call_outer_instruction_index asc, call_inner_instruction_index asc) as log_order
        FROM nested_logs
    )
    
    , trades as (
        SELECT
            case when account_buyer = call_tx_signer then 'buy' else 'sell' end as trade_category
            , buyerPrice as price
            , makerFeeBp
            , takerFeeBp
            , makerFeeRaw
            , takerFeeRaw
            , coalesce(makerFeeBp,makerFeeRaw)/1e4*buyerPrice as maker_fee
            , coalesce(takerFeeBp,takerFeeRaw)/1e4*buyerPrice as taker_fee 
            , tokenSize as token_size
            , rl.royalty --we will just be missing this if log is truncated.
            , trade.call_instruction_name as instruction
            , trade.account_metadata
            , trade.account_tokenMint
            , trade.account_buyer
            , trade.account_seller
            , trade.call_outer_instruction_index as outer_instruction_index
            , trade.call_inner_instruction_index as inner_instruction_index
            , trade.call_block_time
            , trade.call_block_slot
            , trade.call_tx_id
            , trade.call_tx_signer
        FROM (
            SELECT 
                call_instruction_name
                , account_buyer
                , account_seller
                , account_metadata
                , account_tokenMint
                --think we have a decoding issue, I've done a manual coalesce for now https://dune.com/queries/3059777
                , coalesce(cast(buyerPrice as double), cast(bytearray_to_bigint(bytearray_reverse(bytearray_substring(call_data,1+10,8))) as double)) as buyerPrice
                , coalesce(cast(tokenSize as double), cast(bytearray_to_bigint(bytearray_reverse(bytearray_substring(call_data,1+18,8))) as double)) as tokenSize
                , cast(makerFeeBp as double) as makerFeeBp
                , bytearray_to_bigint(bytearray_reverse(case when bytearray_substring(call_data,1+43,1) = 0xff --check second byte for 0xff
                    then rpad(bytearray_substring(call_data,1+42,2), 8, 0xff) else bytearray_substring(call_data,1+42,2) end)) as makerFeeRaw
                , cast(takerFeeBp as double) as takerFeeBp
                , cast(bytearray_to_bigint(bytearray_reverse(bytearray_substring(call_data,1+44,2))) as double) as takerFeeRaw
                , call_outer_instruction_index
                , call_inner_instruction_index
                , call_block_time
                , call_block_slot
                , call_tx_id
                , call_tx_signer
                , row_number() over (partition by call_tx_id order by call_outer_instruction_index asc, call_inner_instruction_index asc) as call_order
            FROM {{ source('magic_eden_solana','m2_call_executeSaleV2') }}
            UNION ALL
            SELECT
                call_instruction_name
                , account_buyer
                , account_seller
                , account_metadata
                , account_tokenMint
                --again, some transactions are missing "args"
                , coalesce(cast(json_value(args, 'strict $.MIP1ExecuteSaleV2Args.price') as double)
                    , cast(bytearray_to_bigint(bytearray_reverse(bytearray_substring(call_data,1+8,8))) as double)
                    ) as buyerPrice
                , 1 as tokenSize
                , cast(json_value(args, 'strict $.MIP1ExecuteSaleV2Args.makerFeeBp') as double) as makerFeeBp
                , bytearray_to_bigint(bytearray_reverse(case when bytearray_substring(call_data,1+17,1) = 0xff --check second byte for 0xff
                    then rpad(bytearray_substring(call_data,1+16,2), 8, 0xff) else bytearray_substring(call_data,1+16,2) end)) as makerFeeRaw
                , cast(json_value(args, 'strict $.MIP1ExecuteSaleV2Args.takerFeeBp') as double) as takerFeeBp
                , cast(bytearray_to_bigint(bytearray_reverse(bytearray_substring(call_data,1+18,2))) as double) as takerFeeRaw
                , call_outer_instruction_index
                , call_inner_instruction_index
                , call_block_time
                , call_block_slot
                , call_tx_id
                , call_tx_signer
                , row_number() over (partition by call_tx_id order by call_outer_instruction_index asc, call_inner_instruction_index asc) as call_order
            FROM {{ source('magic_eden_solana','m2_call_mip1ExecuteSaleV2') }}
            UNION ALL
            SELECT 
                call_instruction_name
                , account_buyer
                , account_seller
                , account_metadata
                , account_tokenMint
                --again, some transactions are missing "args"
                , coalesce(cast(json_value(args, 'strict $.OCPExecuteSaleV2Args.price') as double)
                    , cast(bytearray_to_bigint(bytearray_reverse(bytearray_substring(call_data,1+8,8))) as double)
                    ) as buyerPrice
                , 1 as tokenSize
                , cast(json_value(args, 'strict $.OCPExecuteSaleV2Args.makerFeeBp') as double) as makerFeeBp
                , bytearray_to_bigint(bytearray_reverse(case when bytearray_substring(call_data,1+17,1) = 0xff --check second byte for 0xff
                    then rpad(bytearray_substring(call_data,1+16,2), 8, 0xff) else bytearray_substring(call_data,1+16,2) end)) as makerFeeRaw
                , cast(json_value(args, 'strict $.OCPExecuteSaleV2Args.takerFeeBp') as double) as takerFeeBp
                , cast(bytearray_to_bigint(bytearray_reverse(bytearray_substring(call_data,1+18,2))) as double) as takerFeeRaw
                , call_outer_instruction_index
                , call_inner_instruction_index
                , call_block_time
                , call_block_slot
                , call_tx_id
                , call_tx_signer
                , row_number() over (partition by call_tx_id order by call_outer_instruction_index asc, call_inner_instruction_index asc) as call_order
            FROM {{ source('magic_eden_solana','m2_call_ocpExecuteSaleV2') }}
        ) trade
        --this shortcut ONLY works if you know that a log is only emitted ONCE per call.
        LEFT JOIN royalty_logs rl ON trade.call_tx_id = rl.call_tx_id
            AND trade.call_block_slot = rl.call_block_slot
            AND trade.call_order = rl.log_order 
        {% if is_incremental() %}
        WHERE trade.call_block_time >= NOW() - interval '7' day
        {% endif %}
        -- --QA
        -- WHERE trade.call_tx_id = '4RrNYCNkJCLjvj4unGgo6owJeEg5qxWZJLYnfyiJgFVypashPaJFVgQRivEvcxeEt3xrQUiUM9bm7VP76XtPenxw'
        -- AND trade.call_block_slot = 219668172
    )
    
    , raw_nft_trades as (
        SELECT
            'solana' as blockchain
            , 'magiceden' as project
            , 'v2' as version
            , date_trunc('day', t.call_block_time) as block_date
            , date_trunc('month', t.call_block_time) as block_month
            , t.call_block_time as block_time
            , tk.token_name
            , tk.token_symbol
            , 'secondary' as trade_type
            , token_size as number_of_items --all single trades right now
            , t.trade_category
            , t.account_buyer as buyer
            , t.account_seller as seller
            , t.price as amount_raw
            , t.price/1e9 as amount_original
            -- , t.price/1e9 * sol_p.price as amount_usd
            , 'SOL' as currency_symbol
            , 'So11111111111111111111111111111111111111112' as currency_address
            , t.account_metadata --token id equivalent
            , tk.account_master_edition --token id equivalent
            , tk.account_mint --token id equivalent
            , tk.verified_creator --contract address equivalent
            , tk.collection_mint --contract address equivalent
            , 'TSWAPaqyCSx2KABk68Shruf4rp7CxcNi8hAsbdwmHbN' as project_program_id
            , cast(null as varchar) as aggregator_name
            , cast(null as varchar) as aggregator_address
            , t.call_tx_id as tx_id
            , t.call_block_slot as block_slot
            , t.call_tx_signer as tx_signer
            , t.taker_fee as taker_fee_amount_raw --taker fees = platform fees
            , t.taker_fee/1e9 as taker_fee_amount
            -- , t.taker_fee/1e9 * sol_p.price as taker_fee_amount_usd
            , t.taker_fee/t.price as taker_fee_percentage
            , t.maker_fee as maker_fee_amount_raw
            , t.maker_fee/1e9 as maker_fee_amount
            -- , t.maker_fee/1e9 * sol_p.price as maker_fee_amount_usd
            , t.maker_fee/t.price as maker_fee_percentage
            , t.royalty as royalty_fee_amount_raw 
            , t.royalty/1e9 as royalty_fee_amount
            -- , t.royalty/1e9 * sol_p.price as royalty_fee_amount_usd
            , t.royalty/t.price as royalty_fee_percentage
            , t.instruction
            , t.outer_instruction_index
            , cast(t.inner_instruction_index as double) as inner_instruction_index
        FROM trades t
        LEFT JOIN {{ ref('tokens_solana_nft') }} tk
            ON t.account_metadata = tk.account_metadata
        -- LEFT JOIN {{ source('prices', 'usd') }} sol_p ON sol_p.blockchain = 'solana' and sol_p.symbol = 'SOL' and sol_p.minute = date_trunc('minute', t.call_block_time) --get sol_price
    )

SELECT 
*
, concat(project,'-',trade_category,'-',cast(amount_raw as varchar),'-',cast(outer_instruction_index as varchar),'-',cast(coalesce(inner_instruction_index, 0) as varchar),'-',account_metadata,'-',tx_id) as unique_trade_id
FROM raw_nft_trades
order by block_time asc