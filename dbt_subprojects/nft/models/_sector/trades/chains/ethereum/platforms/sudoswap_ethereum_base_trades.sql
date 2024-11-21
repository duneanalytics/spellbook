{{ config(
    schema = 'sudoswap_ethereum',

    alias = 'base_trades',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_number','tx_hash','sub_tx_trade_id'],
    )
}}
--base table CTEs
WITH
    pairs_created as (
        SELECT
            _nft as nftcontractaddress
            , _initialNFTIDs as nft_ids
            , _fee as initialfee
            , _assetRecipient as asset_recip
            , output_pair as pair_address
            , call_block_time as block_time
            , contract_address as protocolfee_recipient -- the factory used to create the pair is the protocol fee recipient
        FROM {{ source('sudo_amm_ethereum','LSSVMPairFactory_call_createPairETH') }}
        WHERE call_success
    )

   , swaps as (
        SELECT
            *
        FROM (
            SELECT
                contract_address
                , call_tx_hash
                , call_trace_address
                , call_block_time
                , call_block_number
                , call_success
                , tokenRecipient as trade_recipient
                , 'Sell' as trade_category
                , isRouter as called_from_router
                , routerCaller as router_caller
            FROM {{ source('sudo_amm_ethereum','LSSVMPair_general_call_swapNFTsForToken') }}
            WHERE call_success = true
            {% if is_incremental() %}
            -- this filter will only be applied on an incremental run. We only want to update with new swaps.
            AND {{incremental_predicate('call_block_time')}}
            {% endif %}

            UNION ALL
            SELECT
                contract_address
                , call_tx_hash
                , call_trace_address
                , call_block_time
                , call_block_number
                , call_success
                , nftRecipient as trade_recipient
                , 'Buy' as trade_category
                , isRouter as called_from_router
                , routerCaller as router_caller
            FROM {{ source('sudo_amm_ethereum','LSSVMPair_general_call_swapTokenForAnyNFTs') }}
            WHERE call_success = true
            {% if is_incremental() %}
            -- this filter will only be applied on an incremental run. We only want to update with new swaps.
            AND {{incremental_predicate('call_block_time')}}
            {% endif %}

            UNION ALL
            SELECT
                contract_address
                , call_tx_hash
                , call_trace_address
                , call_block_time
                , call_block_number
                , call_success
                , nftRecipient as trade_recipient
                , 'Buy' as trade_category
                , isRouter as called_from_router
                , routerCaller as router_caller
            FROM {{ source('sudo_amm_ethereum','LSSVMPair_general_call_swapTokenForSpecificNFTs') }}
            WHERE call_success = true
            {% if is_incremental() %}
            -- this filter will only be applied on an incremental run. We only want to update with new swaps.
            AND {{incremental_predicate('call_block_time')}}
            {% endif %}
        ) s
    )

    -- this join should be removed in the future when more call trace info is added to the _call_ tables, we need the call_from field to track down the eth traces.
    , swaps_with_calldata as (
        select s.*
        , tr."from" as call_from
        , CASE WHEN called_from_router = true THEN tr."from" ELSE tr.to END as project_contract_address -- either the router or the pool if called directly
        from swaps s
        inner join {{ source('ethereum', 'traces') }} tr
        ON tr.success and s.call_block_number = tr.block_number and s.call_tx_hash = tr.tx_hash and s.call_trace_address = tr.trace_address
        {% if is_incremental() %}
        -- this filter will only be applied on an incremental run. We only want to update with new swaps.
        AND {{incremental_predicate('tr.block_time')}}
        {% else %}
        AND tr.block_time >= TIMESTAMP '2022-4-1'
        {% endif %}
    )


    ,pool_fee_update as (
        SELECT
            *
        FROM {{ source('sudo_amm_ethereum','LSSVMPair_general_evt_FeeUpdate') }}
    )

    ,protocol_fee_update as (
        SELECT
            *
        FROM {{ source('sudo_amm_ethereum','LSSVMPairFactory_evt_ProtocolFeeMultiplierUpdate') }}
    )

    ,asset_recipient_update as (
        SELECT
            *
        FROM {{ source('sudo_amm_ethereum','LSSVMPair_general_evt_AssetRecipientChange') }}
    )

--logic CTEs
    ,swaps_w_fees as (
        SELECT
            *
        FROM (
            SELECT
                call_tx_hash
                , call_block_time
                , call_block_number
                , contract_address as pair_address
                , call_trace_address
                , call_from
                , router_caller
                , pool_fee
                , protocolfee
                , protocolfee_recipient
                , trade_category
                , nftcontractaddress
                , asset_recip
                , trade_recipient
                , project_contract_address
                , row_number() OVER (partition by call_tx_hash, contract_address, call_trace_address order by fee_update_time desc, protocolfee_update_time desc, asset_recip_update_time desc) as ordering
            FROM (
                SELECT
                    swaps.*
                    , COALESCE(fu.newfee, pc.initialfee)/1e18 as pool_fee --most recent pool_fee, depends on bonding curve to implement it correctly. See explanation in fee table schema.
                    , COALESCE(fu.evt_block_time, pc.block_time) as fee_update_time
                    , pfu.newMultiplier/1e18 as protocolfee --most recent protocolfee, depends on bonding curve to implement it correctly. See explanation in fee table schema.
                    , pfu.evt_block_time as protocolfee_update_time
                    , pc.protocolfee_recipient
                    , pc.nftcontractaddress
                    , coalesce(aru.a, pc.asset_recip) as asset_recip
                    , coalesce(aru.evt_block_time, pc.block_time) as asset_recip_update_time
                FROM swaps_with_calldata swaps
                JOIN pairs_created pc ON pc.pair_address = contract_address --remember swaps from other NFT addresses won't appear!
                -- we might need to do these joins separately since we're exploding into a lot of rows..
                -- should not matter a lot since # of changes per pool should be small
                LEFT JOIN pool_fee_update fu ON swaps.call_block_time >= fu.evt_block_time AND swaps.contract_address = fu.contract_address
                LEFT JOIN protocol_fee_update pfu ON swaps.call_block_time >= pfu.evt_block_time
                LEFT JOIN asset_recipient_update aru on swaps.call_block_time >= aru.evt_block_time AND swaps.contract_address = aru.contract_address
            ) a
        ) b
        WHERE ordering = 1 --we want to keep the most recent pool_fee and protocol fee for each individual call (trade)
    )

    ,swaps_w_traces as (
        -- we traces to get NFT and ETH transfer data because sudoswap doesn't emit any data in events for swaps, so we have to piece it together manually based on trace_address.
        SELECT
            sb.call_block_time
            , sb.call_block_number
            , sb.trade_category
            , cast(SUM(
                CASE WHEN sb.trade_category = 'Buy' -- caller buys, AMM sells
                THEN (
                    CASE WHEN tr."from" = sb.call_from THEN cast(value as int256) -- amount of ETH payed
                    WHEN (tr.to = sb.call_from AND sb.call_from != sb.asset_recip) THEN -cast(value as int256) --refunds unless the caller is also the asset recipient, no way to discriminate there.
                    ELSE int256 '0' END)
                ELSE ( -- caller sells, AMM buys
                    CASE WHEN tr."from" = sb.pair_address THEN cast(value as int256) -- all ETH leaving the pool, nothing should be coming in on a sell.
                    ELSE int256 '0' END)
                END ) as uint256) as trade_price -- what the buyer paid (incl all fees)
            , SUM(
                CASE WHEN (tr.to = sb.protocolfee_recipient) THEN cast(value as uint256)
                ELSE uint256 '0' END
                 ) as protocol_fee_amount -- what the buyer paid
            , filter(ARRAY_AGG(distinct CASE WHEN bytearray_substring(input,1,4)=0x42842e0e THEN bytearray_to_uint256(bytearray_substring(input,69,32)) END)
                , x->x is not null
                ) as nft_token_id
            , sb.call_tx_hash
            , sb.trade_recipient
            , sb.pair_address
            , sb.nftcontractaddress
            , sb.pool_fee
            , sb.protocolfee
            , sb.protocolfee_recipient
            , project_contract_address
            -- these 2 are used for matching the aggregator address, dropped later
            , router_caller
            , call_from
        FROM swaps_w_fees sb
        INNER JOIN {{ source('ethereum', 'traces') }} tr
            ON tr.type = 'call'
            AND tr.call_type = 'call'
            AND success
            AND tr.block_number = sb.call_block_number
            AND tr.tx_hash = sb.call_tx_hash
            AND (
                (cardinality(call_trace_address) != 0
                AND call_trace_address = slice(tr.trace_address,1,cardinality(call_trace_address))
                ) --either a normal tx where trace address helps us narrow down which subtraces to look at for ETH transfers or NFT transfers.
                OR cardinality(call_trace_address) = 0 -- In this case the swap function was called directly, all traces are thus subtraces of that call (like 0x34a52a94fce15c090cc16adbd6824948c731ecb19a39350633590a9cd163658b).
                )
            {% if is_incremental() %}
            AND {{incremental_predicate('tr.block_time')}}
            {% endif %}
            {% if not is_incremental() %}
            AND tr.block_time >= TIMESTAMP '2022-4-1'
            {% endif %}
        GROUP BY 1,2,3,7,8,9,10,11,12,13,14,15,16
    )

    ,swaps_cleaned as (
        SELECT
             call_block_time as block_time
            , call_block_number as block_number
            , nft_token_id
            , cardinality(nft_token_id) as number_of_items
            , 'secondary' as trade_type
            , trade_category
            , CASE WHEN trade_category = 'Buy' THEN pair_address --AMM is selling if an NFT is being bought
                ELSE trade_recipient
                END as seller
            , CASE WHEN trade_category = 'Sell' THEN pair_address --AMM is buying if an NFT is being sold
                ELSE trade_recipient
                END as buyer
            , trade_price as price_raw
            , {{var("ETH_ERC20_ADDRESS")}} as currency_contract --ETH
            , nftcontractaddress as nft_contract_address
            , project_contract_address -- This is either the router or the pool address if called directly
            , call_tx_hash as tx_hash
            , protocol_fee_amount as platform_fee_amount_raw
            , protocolfee_recipient
             -- trade_price = baseprice + (baseprice*pool_fee) + (baseprice*protocolfee)
            , (trade_price-protocol_fee_amount)/(1+pool_fee)*pool_fee as pool_fee_amount_raw
        FROM swaps_w_traces
    )

SELECT
     'ethereum' as blockchain
    , 'sudoswap' as project
    , 'v1' as project_version
    , block_time
    , block_number
    , tx_hash
    , project_contract_address
    , buyer
    , seller
    , nft_contract_address
    , one_nft_token_id as nft_token_id --nft.trades prefers each token id be its own row
    , uint256 '1' as nft_amount
    , trade_type
    , trade_category
    , currency_contract
    , cast(price_raw/number_of_items as uint256) as price_raw
    , cast(platform_fee_amount_raw/number_of_items as uint256) as platform_fee_amount_raw
    , uint256 '0' as royalty_fee_amount_raw
    , cast(pool_fee_amount_raw/number_of_items as uint256) as pool_fee_amount_raw
    , protocolfee_recipient as platform_fee_address
    , cast(null as varbinary) as royalty_fee_address
    , row_number() over (partition by tx_hash order by one_nft_token_id) as sub_tx_trade_id
FROM swaps_cleaned
CROSS JOIN UNNEST(nft_token_id) as foo(one_nft_token_id)

