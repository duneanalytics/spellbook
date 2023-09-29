{{
    config(
        schema = 'tensorswap_solana'
        , tags = ['dunesql']
        , alias = alias('trades')
        ,materialized = 'incremental'
        ,file_format = 'delta'
        ,incremental_strategy = 'merge'
        ,unique_key = ['project','trade_category','outer_instruction_index','inner_instruction_index','account_metadata','tx_id']
        ,post_hook='{{ expose_spells(\'["solana"]\',
                                    "project",
                                    "tensorswap",
                                    \'["ilemi"]\') }}'
    )
}}

with 
    logs as (
        with nested_logs as (
            SELECT 
                distinct 
                call_tx_id
                , call_block_slot
                , call_outer_instruction_index
                , call_inner_instruction_index
                , from_base64(split(logs, ' ')[3]) as hex_data
                , logs
            FROM (
                SELECT
                    call_tx_id
                    , call_block_slot
                    , call_outer_instruction_index
                    , call_inner_instruction_index
                    , call_log_messages
                FROM {{ source('tensorswap_v1_solana','tensorswap_call_buyNft') }}
                UNION ALL 
                SELECT
                    call_tx_id
                    , call_block_slot
                    , call_outer_instruction_index
                    , call_inner_instruction_index
                    , call_log_messages
                FROM {{ source('tensorswap_v1_solana','tensorswap_call_buySingleListing') }}
                UNION ALL 
                SELECT
                    call_tx_id
                    , call_block_slot
                    , call_outer_instruction_index
                    , call_inner_instruction_index
                    , call_log_messages
                FROM {{ source('tensorswap_v1_solana','tensorswap_call_sellNftTokenPool') }}
                UNION ALL 
                SELECT
                    call_tx_id
                    , call_block_slot
                    , call_outer_instruction_index
                    , call_inner_instruction_index
                    , call_log_messages
                FROM {{ source('tensorswap_v1_solana','tensorswap_call_sellNftTradePool') }}
            ) LEFT JOIN unnest(call_log_messages) as log_messages(logs) ON True
            WHERE logs LIKE '%Program data:%'
            AND try(from_base64(split(logs, ' ')[3])) is not null --valid hex
            AND bytearray_substring(from_base64(split(logs, ' ')[3]), 1, 8) = 0x62d0783c5d2013b4 --BuySellEvent
        )
        
        SELECT
            'BuySellEvent' as event
            , bytearray_substring(hex_data, 1, 8) as discriminator
            , bytearray_to_bigint(bytearray_reverse(bytearray_substring(hex_data, 9, 8))) as current_price
            , bytearray_to_bigint(bytearray_reverse(bytearray_substring(hex_data, 17, 8))) as tswap_fee
            , bytearray_to_bigint(bytearray_reverse(bytearray_substring(hex_data, 25, 8))) as mm_fee
            , bytearray_to_bigint(bytearray_reverse(bytearray_substring(hex_data, 33, 8))) as creators_fee
            , call_tx_id
            , call_block_slot
            , call_outer_instruction_index
            , call_inner_instruction_index
            , row_number() over (partition by call_tx_id order by call_outer_instruction_index asc, call_inner_instruction_index asc) as log_order
            , logs
        FROM nested_logs
    )
    
    , trades as (
        SELECT
            rl.event
            , coalesce(cast(rl.current_price as double), try(cast(json_value(config, 'strict $.startingPrice') as double))) as current_price
            , coalesce(cast(rl.tswap_fee as double), try(cast(json_value(config, 'strict $.startingPrice') as double))*0.014) as tswap_fee --taker fee is 1.4% right now.
            , -1*coalesce(cast(rl.mm_fee as double)
                    , try(cast(json_value(config, 'strict $.startingPrice') as double)*try(cast(json_value(config, 'strict $.mmFeeBps') as double)/1e4))
                    ) as mm_fee --maker fee goes back to users
            , cast(rl.creators_fee as double) as creators_fee --we will just be missing this if log is truncated.
            , trade.call_instruction_name as instruction
            , trade.trade_category
            , trade.account_metadata
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
                call_account_arguments[7] as account_metadata
                , call_account_arguments[12] as account_buyer
                , call_account_arguments[11] as account_seller
                , config
                , 'buy' as trade_category
                , call_tx_id
                , call_outer_instruction_index
                , call_inner_instruction_index
                , call_block_slot
                , call_instruction_name
                , call_block_time
                , call_tx_signer
                , row_number() over (partition by call_tx_id order by call_outer_instruction_index asc, call_inner_instruction_index asc) as call_order
            FROM {{ source('tensorswap_v1_solana','tensorswap_call_buyNft') }}
            UNION ALL
            SELECT 
                call_account_arguments[6] as account_metadata
                , call_account_arguments[9] as account_buyer
                , call_account_arguments[8] as account_seller
                , null --no price fallback fyi. fix later with spl token join (too expensive for now)
                , 'buy' as trade_category
                , call_tx_id
                , call_outer_instruction_index
                , call_inner_instruction_index
                , call_block_slot
                , call_instruction_name
                , call_block_time
                , call_tx_signer
                , row_number() over (partition by call_tx_id order by call_outer_instruction_index asc, call_inner_instruction_index asc) as call_order
            FROM {{ source('tensorswap_v1_solana','tensorswap_call_buySingleListing') }}
            UNION ALL
            SELECT 
                call_account_arguments[8] as account_metadata
                , call_account_arguments[10] as account_buyer --pnft shared
                , call_account_arguments[2] as account_seller --owner ata account
                , config
                , 'sell' as trade_category
                , call_tx_id
                , call_outer_instruction_index
                , call_inner_instruction_index
                , call_block_slot
                , call_instruction_name
                , call_block_time
                , call_tx_signer
                , row_number() over (partition by call_tx_id order by call_outer_instruction_index asc, call_inner_instruction_index asc) as call_order
            FROM {{ source('tensorswap_v1_solana','tensorswap_call_sellNftTokenPool') }}
            UNION ALL
            SELECT 
                call_account_arguments[8] as account_metadata
                , call_account_arguments[1] as account_buyer --shared
                , call_account_arguments[11] as account_seller --pnftshared
                , config
                , 'sell' as trade_category
                , call_tx_id
                , call_outer_instruction_index
                , call_inner_instruction_index
                , call_block_slot
                , call_instruction_name
                , call_block_time
                , call_tx_signer
                , row_number() over (partition by call_tx_id order by call_outer_instruction_index asc, call_inner_instruction_index asc) as call_order
            FROM {{ source('tensorswap_v1_solana','tensorswap_call_sellNftTradePool') }}
        ) trade
        --this shortcut ONLY works if you know that a log is only emitted ONCE per call.
        LEFT JOIN logs rl ON trade.call_tx_id = rl.call_tx_id
            AND trade.call_block_slot = rl.call_block_slot
            AND trade.call_order = rl.log_order 
        WHERE 1=1
    )
    
    , raw_nft_trades as (
        SELECT
            'solana' as blockchain
            , 'tensor' as project
            , 'v1' as version
            , t.call_block_time as block_time
            , tk.token_name
            , tk.token_symbol
            , 'secondary' as trade_type
            , 1 as number_of_items
            , t.trade_category
            , t.account_buyer as buyer
            , t.account_seller as seller
            , t.current_price as amount_raw
            , t.current_price/1e9 as amount_original
            , t.current_price/1e9 * sol_p.price as amount_usd
            , 'SOL' as currency_symbol
            , 'So11111111111111111111111111111111111111112' as currency_address
            , t.account_metadata
            , tk.account_master_edition 
            , tk.account_mint
            , tk.verified_creator
            , tk.collection_mint
            , 'TSWAPaqyCSx2KABk68Shruf4rp7CxcNi8hAsbdwmHbN' as project_program_id
            , cast(null as varchar) as aggregator_name
            , cast(null as varchar) as aggregator_address
            , t.call_tx_id as tx_id
            , t.call_block_slot as block_slot
            , t.call_tx_signer as tx_signer
            --taker fees = platform fees
            , t.tswap_fee as taker_fee_amount_raw
            , t.tswap_fee/1e9 as taker_fee_amount
            , t.tswap_fee/1e9 * sol_p.price as taker_fee_amount_usd
            , t.tswap_fee/t.current_price as taker_fee_percentage
            , t.mm_fee as maker_fee_amount_raw
            , t.mm_fee/1e9 as maker_fee_amount
            , t.mm_fee/1e9 * sol_p.price as maker_fee_amount_usd
            , t.mm_fee/t.current_price as maker_fee_percentage
            , t.creators_fee as royalty_fee_amount_raw 
            , t.creators_fee/1e9 as royalty_fee_amount
            , t.creators_fee/1e9 * sol_p.price as royalty_fee_amount_usd
            , t.creators_fee/t.current_price as royalty_fee_percentage
            , t.instruction
            , t.outer_instruction_index
            , coalesce(t.inner_instruction_index, 0) as inner_instruction_index
        FROM trades t
        LEFT JOIN {{ ref('tokens_solana_nft') }} tk
            ON t.account_metadata = tk.account_metadata
        LEFT JOIN {{ source('prices', 'usd') }} sol_p ON sol_p.blockchain = 'solana' and sol_p.symbol = 'SOL' and sol_p.minute = date_trunc('minute', t.call_block_time) --get sol_price
    )

SELECT 
*
, concat(project,'-',trade_category,'-',cast(outer_instruction_index as varchar),'-',cast(coalesce(inner_instruction_index,0) as varchar),'-',account_metadata,'-',tx_id) as unique_trade_id
FROM raw_nft_trades
--we have some truncated logs and missing decoding right now like 5DoPTZfA9UfSJYExLhvkMKmTtLXCjumH7dfUVY6gpLc7Bj99kg3Z7649eKgh1x5aARTbsMWPs1XEkwC3up4BByUv
WHERE amount_original is not null
order by block_time asc