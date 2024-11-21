{{
    config(
        schema = 'tensorswap_v2_solana'
        
        , alias = 'trades'
        ,materialized = 'incremental'
        ,file_format = 'delta'
        ,incremental_strategy = 'merge'
        ,incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
        ,unique_key = ['project','trade_category','outer_instruction_index','inner_instruction_index','account_merkle_tree','leaf_id','tx_id']
        ,post_hook='{{ expose_spells(\'["solana"]\',
                                    "project",
                                    "tensorswap",
                                    \'["ilemi"]\') }}'
    )
}}

with 
cnft_base as (
        -- explorer https://xray.helius.xyz/token/3d5Le48C7fSmCanRQ1UUCV3nLpv9rCdMMtsyzihvizx2
        -- mint https://solscan.io/tx/61LqYDXKBYxsZEJoRBCaP8mvbeKGfbezZ54YUj1JZxPN7hGgr1t5rqseTQzxvkTu72YxkeFWq4bcFpcJBTCBNcfs
        -- trade https://solscan.io/tx/22jXeGFXSvSnnGtVMnt17Ve7Z462A8yUxMeni3617jv3BoTLYRY2LVb3fzMHT9wwrrMYdDYwbhrgM3dfcY48GF65
        SELECT
            case when maxAmount/1e9 > 1e5 --there is something weird in decoding where sometimes the maxAmount is decoded wrong https://solscan.io/tx/BDmz29bRCdist8ZX2LZHa9Lbsygfp7JpC9UaMpNWCQgDiRfMqvs9PR9b1Sghkg1yUUPn7oN71tYG3TxTkBMhsaC
                then cast(maxAmount as double)/1e9
                else cast(maxAmount as double)
                end as price
            , (case when maxAmount/1e9 > 1e5
                then cast(maxAmount as double)/1e9
                else cast(maxAmount as double)
                end)
                *0.014 as taker_fee --taker fee is 1.4% right now.
            , 0 as maker_fee --maker fee goes back to users
            , (case when maxAmount/1e9 > 1e5
                then cast(maxAmount as double)/1e9
                else cast(maxAmount as double)
                end)
                *sellerFeeBasisPoints/10000 as royalty_fee
            , call_instruction_name as instruction
            , case when call_tx_signer = account_buyer then 'buy' else 'sell' end as trade_category
            , account_merkleTree as account_merkle_tree
            , bytearray_to_bigint(bytearray_reverse(bytearray_substring(call_data,1+8+8,4))) as leaf_id --index is sometimes empty. idk what difference is with nonce
            , account_buyer as buyer
            , account_owner as seller
            , call_outer_instruction_index as outer_instruction_index
            , coalesce(call_inner_instruction_index, 0) as inner_instruction_index
            , call_block_time as block_time
            , call_block_slot as block_slot
            , call_tx_id as tx_id
            , call_tx_signer as tx_signer
        FROM {{ source('tensor_cnft_solana','tcomp_call_buy') }}
        {% if is_incremental() %}
        WHERE {{incremental_predicate('call_block_time')}}
        {% endif %}

        UNION ALL 

        SELECT 
            cast(minAmount as double) as price
            , cast(minAmount as double)*0.014 as taker_fee --taker fee is 1.4% right now.
            , 0 as maker_fee --maker fee goes back to users
            , cast(minAmount as double)*cast(json_value(metaArgs, 'strict $.TMetadataArgs.sellerFeeBasisPoints') as double)/10000 as royalty_fee
            , call_instruction_name as instruction
            , 'sell' as trade_category
            , account_merkleTree
            , bytearray_to_bigint(bytearray_reverse(bytearray_substring(call_data,1+8+8,4))) as leaf_id
            , account_owner as buyer
            , account_seller as seller
            , call_outer_instruction_index as outer_instruction_index
            , coalesce(call_inner_instruction_index, 0) as inner_instruction_index
            , call_block_time as block_time
            , call_block_slot as block_slot
            , call_tx_id as tx_id
            , call_tx_signer as tx_signer
        FROM {{ source('tensor_cnft_solana','tcomp_call_takeBidFullMeta') }}
        {% if is_incremental() %}
        WHERE {{incremental_predicate('call_block_time')}}
        {% endif %}

        UNION ALL 
        
        SELECT 
            cast(minAmount as double) as price
            , cast(minAmount as double)*0.014 as taker_fee --taker fee is 1.4% right now.
            , 0 as maker_fee --maker fee goes back to users
            , cast(minAmount as double)*sellerFeeBasisPoints/10000 as royalty_fee
            , call_instruction_name as instruction
            , 'sell' as trade_category
            , account_merkleTree
            , bytearray_to_bigint(bytearray_reverse(bytearray_substring(call_data,1+8+8,4))) as leaf_id
            , account_owner as buyer
            , account_seller as seller
            , call_outer_instruction_index as outer_instruction_index
            , coalesce(call_inner_instruction_index, 0) as inner_instruction_index
            , call_block_time as block_time
            , call_block_slot as block_slot
            , call_tx_id as tx_id
            , call_tx_signer as tx_signer
        FROM {{ source('tensor_cnft_solana','tcomp_call_takeBidMetaHash') }}
        {% if is_incremental() %}
        WHERE {{incremental_predicate('call_block_time')}}
        {% endif %}
    )
    
SELECT
    'solana' as blockchain
    , 'tensorswap' as project
    , 'v2' as version
    , t.block_time
    , 'secondary' as trade_type
    , 1 as number_of_items --all single trades right now
    , t.trade_category
    , t.buyer
    , t.seller
    , t.price as amount_raw
    , t.price/1e9 as amount_original
    , t.price/1e9 * sol_p.price as amount_usd
    , 'SOL' as currency_symbol
    , 'So11111111111111111111111111111111111111112' as currency_address
    , t.account_merkle_tree  --token id equivalent
    , cast(t.leaf_id as bigint) as leaf_id --token id equivalent
    , cast(null as varchar) as account_mint
    , 'TCMPhJdwDryooaGtiocG1u3xcYbRpiJzb283XfCZsDp' as project_program_id
    , cast(null as varchar) as aggregator_name
    , cast(null as varchar) as aggregator_address
    , t.tx_id
    , t.block_slot
    , t.tx_signer
    --taker fees = platform fees. note that if selling to a pool, platform takes full taker fee. else, maker fee is paid out from taker fee.
    , t.taker_fee as taker_fee_amount_raw
    , t.taker_fee/1e9 as taker_fee_amount
    , t.taker_fee/1e9 * sol_p.price as taker_fee_amount_usd
    , case when t.taker_fee = 0 OR t.price = 0 then 0 else t.taker_fee/t.price end as taker_fee_percentage
    , t.maker_fee as maker_fee_amount_raw
    , t.maker_fee/1e9 as maker_fee_amount
    , t.maker_fee/1e9 * sol_p.price as maker_fee_amount_usd
    , case when t.maker_fee = 0 OR t.price = 0 then 0 else t.maker_fee/t.price end as maker_fee_percentage
    , cast(null as double) as amm_fee_amount_raw
    , cast(null as double) as amm_fee_amount
    , cast(null as double) as amm_fee_amount_usd
    , cast(null as double) as amm_fee_percentage
    , t.royalty_fee as royalty_fee_amount_raw 
    , t.royalty_fee/1e9 as royalty_fee_amount
    , t.royalty_fee/1e9 * sol_p.price as royalty_fee_amount_usd
    , case when t.royalty_fee = 0 OR t.price = 0 then 0 else t.royalty_fee/t.price end as royalty_fee_percentage
    , t.instruction
    , t.outer_instruction_index
    , t.inner_instruction_index
FROM cnft_base t
LEFT JOIN {{ source('prices', 'usd') }} sol_p ON sol_p.blockchain = 'solana' and sol_p.symbol = 'SOL' and sol_p.minute = date_trunc('minute', t.block_time) --get sol_price