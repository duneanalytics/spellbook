{{
    config(
        schema = 'tensorswap_v2_solana'
        , tags = ['dunesql']
        , alias = alias('trades')
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
            cast(maxAmount as double) as price
            , cast(maxAmount as double)*0.014 as taker_fee --taker fee is 1.4% right now.
            , 0 as maker_fee --maker fee goes back to users
            , cast(maxAmount as double)*sellerFeeBasisPoints/10000 as royalty_fee
            , call_instruction_name as instruction
            , case when call_tx_signer = account_buyer then 'buy' else 'sell' end as trade_category
            , account_merkleTree as account_merkle_tree
            , index as leaf_id
            , account_buyer as buyer
            , account_owner as seller
            , call_outer_instruction_index as outer_instruction_index
            , call_inner_instruction_index as inner_instruction_index
            , call_block_time as block_time
            , call_block_slot as block_slot
            , call_tx_id as tx_id
            , call_tx_signer as tx_signer
        FROM {{ source('tensor_cnft_solana','tcomp_call_buy') }}
        {% if is_incremental() %}
        WHERE {{incremental_predicate('call_block_time')}}
        {% endif %}
    )
    
SELECT
    'solana' as blockchain
    , 'tensor' as project
    , 'v2' as version
    , t.block_time
    , tk.token_name
    , tk.token_symbol
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
    , t.account_merkleTree as account_merkle_tree  --token id equivalent
    , cast(t.leaf_id as bigint) as leaf_id --token id equivalent
    , cast(null as varchar) as account_metadata
    , cast(null as varchar) as account_master_edition
    , cast(null as varchar) as account_mint
    , tk.verified_creator
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
    , t.taker_fee/COALESCE(t.price,1) as taker_fee_percentage
    , t.maker_fee as maker_fee_amount_raw
    , t.maker_fee/1e9 as maker_fee_amount
    , t.maker_fee/1e9 * sol_p.price as maker_fee_amount_usd
    , t.maker_fee/COALESCE(t.price,1) as maker_fee_percentage
    , cast(null as double) as amm_fee_amount_raw
    , cast(null as double) as amm_fee_amount
    , cast(null as double) as amm_fee_amount_usd
    , cast(null as double) as amm_fee_percentage
    , t.royalty_fee as royalty_fee_amount_raw 
    , t.royalty_fee/1e9 as royalty_fee_amount
    , t.royalty_fee/1e9 * sol_p.price as royalty_fee_amount_usd
    , t.royalty_fee/COALESCE(t.price,1) as royalty_fee_percentage
    , t.instruction
    , t.outer_instruction_index
    , t.inner_instruction_index
FROM cnft_base t
left join {{ ref('tokens_solana_nft') }} tk on tk.account_merkle_tree = t.account_merkleTree and tk.leaf_id = t.leaf_id
LEFT JOIN {{ source('prices', 'usd') }} sol_p ON sol_p.blockchain = 'solana' and sol_p.symbol = 'SOL' and sol_p.minute = date_trunc('minute', t.block_time) --get sol_price