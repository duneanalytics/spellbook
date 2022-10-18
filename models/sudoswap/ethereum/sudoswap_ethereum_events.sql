{{ config(
        alias = 'events',
        partition_by = ['block_date'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['block_date', 'unique_trade_id'],
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "project",
                                    "sudoswap",
                                    \'["ilemi"]\') }}'
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
            AND call_block_time >= date_trunc("day", now() - interval '1 week')
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
            AND call_block_time >= date_trunc("day", now() - interval '1 week')
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
            AND call_block_time >= date_trunc("day", now() - interval '1 week')
            {% endif %}
        ) s
    )

    -- this join should be removed in the future when more call trace info is added to the _call_ tables, we need the call_from field to track down the eth traces.
    , swaps_with_calldata as (
        select s.*
        , tr.from as call_from
        , CASE WHEN called_from_router = true THEN tr.from ELSE tr.to END as project_contract_address -- either the router or the pool if called directly
        from swaps s
        inner join {{ source('ethereum', 'traces') }} tr
        ON tr.success and s.call_block_number = tr.block_number and s.call_tx_hash = tr.tx_hash and s.call_trace_address = tr.trace_address
        {% if is_incremental() %}
        -- this filter will only be applied on an incremental run. We only want to update with new swaps.
        AND tr.block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
        {% if not is_incremental() %}
        AND tr.block_time >= '2022-4-1'
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

    ,tokens_ethereum_nft as (
        SELECT
            *
        FROM {{ ref('tokens_nft') }}
        WHERE blockchain = 'ethereum'
    )

    ,nft_ethereum_aggregators as (
        SELECT
            *
        FROM {{ ref('nft_aggregators') }}
        WHERE blockchain = 'ethereum'
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
            , SUM(
                CASE WHEN sb.trade_category = 'Buy' -- caller buys, AMM sells
                THEN (
                    CASE WHEN tr.from = sb.call_from THEN value -- amount of ETH payed
                    WHEN (tr.to = sb.call_from AND sb.call_from != sb.asset_recip) THEN -value --refunds unless the caller is also the asset recipient, no way to discriminate there.
                    ELSE 0 END)
                ELSE ( -- caller sells, AMM buys
                    CASE WHEN tr.from = sb.pair_address THEN value -- all ETH leaving the pool, nothing should be coming in on a sell.
                    ELSE 0 END)
                END ) as trade_price -- what the buyer paid (incl all fees)
            , SUM(
                CASE WHEN (tr.to = sb.protocolfee_recipient) THEN value
                ELSE 0 END
                 ) as protocol_fee_amount -- what the buyer paid
            , ARRAY_AGG(distinct CASE WHEN substring(input,1,10)='0x42842e0e' THEN bytea2numeric_v2(substring(input,139,64))::int ELSE null::int END)
                as token_id
            , sb.call_tx_hash
            , sb.trade_recipient
            , sb.pair_address
            , sb.nftcontractaddress
            , sb.pool_fee
            , sb.protocolfee
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
                (cardinality(call_trace_address) != 0 AND call_trace_address = slice(tr.trace_address,1,cardinality(call_trace_address))) --either a normal tx where trace address helps us narrow down which subtraces to look at for ETH transfers or NFT transfers.
                OR cardinality(call_trace_address) = 0 -- In this case the swap function was called directly, all traces are thus subtraces of that call (like 0x34a52a94fce15c090cc16adbd6824948c731ecb19a39350633590a9cd163658b).
                )
            {% if is_incremental() %}
            AND tr.block_time >= date_trunc("day", now() - interval '1 week')
            {% endif %}
            {% if not is_incremental() %}
            AND tr.block_time >= '2022-4-1'
            {% endif %}
        GROUP BY 1,2,3,7,8,9,10,11,12,13,14,15
    )

    ,swaps_cleaned as (
        --formatting swaps for sudoswap_ethereum_events defined schema
        SELECT
            'ethereum' as blockchain
            , 'sudoswap' as project
            , 'v1' as version
            , TRY_CAST(date_trunc('DAY', call_block_time) AS date) AS block_date
            , call_block_time as block_time
            , call_block_number as block_number
            , token_id
            , 'erc721' as token_standard
            , cardinality(token_id) as number_of_items
            , CASE WHEN cardinality(token_id) > 1 THEN 'Bundle Trade'
                ELSE 'Single Item Trade'
               END as trade_type
            , trade_category
            , 'Trade' as evt_type
            , CASE WHEN trade_category = 'Buy' THEN pair_address --AMM is selling if an NFT is being bought
                ELSE trade_recipient
                END as seller
            , CASE WHEN trade_category = 'Sell' THEN pair_address --AMM is buying if an NFT is being sold
                ELSE trade_recipient
                END as buyer
            , trade_price as amount_raw
            , trade_price/1e18 as amount_original
            , 'ETH' as currency_symbol
            , '0x0000000000000000000000000000000000000000' as currency_contract --ETH
            , nftcontractaddress as nft_contract_address
            , project_contract_address -- This is either the router or the pool address if called directly
            , call_tx_hash as tx_hash
            , '' as evt_index --we didn't use events in our case for decoding, so this will be null until we find a way to tie it together.
            , protocol_fee_amount as platform_fee_amount_raw
            , protocol_fee_amount/1e18 as platform_fee_amount
            , protocolfee as platform_fee_percentage
             -- trade_price = baseprice + (baseprice*pool_fee) + (baseprice*protocolfee)
            , (trade_price-protocol_fee_amount)/(1+pool_fee)*pool_fee as pool_fee_amount_raw
            , (trade_price-protocol_fee_amount)/(1+pool_fee)*pool_fee/1e18 as pool_fee_amount
            , pool_fee as pool_fee_percentage
            -- royalties don't currently exist on the AMM,
            , null::double as royalty_fee_amount_raw
            , null::double as royalty_fee_amount
            , null::double as royalty_fee_percentage
            , null::string as royalty_fee_receive_address
            , null::double as royalty_fee_amount_usd
            , null::string as royalty_fee_currency_symbol
            -- these 2 are used for matching the aggregator address, dropped later
            , router_caller
            , call_from
        FROM swaps_w_traces
    )

    ,swaps_cleaned_w_metadata as (
        SELECT
            sc.*
            , tokens.name AS collection
            , case when lower(right(tx.data, 8)) = '72db8c0b' then 'Gem' else agg.name end as aggregator_name
            , agg.contract_address as aggregator_address
            , sc.amount_original*pu.price as amount_usd
            , sc.pool_fee_amount*pu.price as pool_fee_amount_usd
            , sc.platform_fee_amount*pu.price as platform_fee_amount_usd
            , tx.from as tx_from
            , tx.to as tx_to
        FROM swaps_cleaned sc
        INNER JOIN {{ source('ethereum', 'transactions') }} tx
            ON tx.block_number=sc.block_number and tx.hash=sc.tx_hash
            {% if is_incremental() %}
            AND tx.block_time >= date_trunc("day", now() - interval '1 week')
            {% endif %}
            {% if not is_incremental() %}
            AND tx.block_time >= '2022-4-1'
            {% endif %}
        LEFT JOIN {{ source('prices', 'usd') }} pu ON pu.blockchain='ethereum'
            AND date_trunc('minute', pu.minute)=date_trunc('minute', sc.block_time)
            AND symbol = 'WETH'
            {% if is_incremental() %}
            AND pu.minute >= date_trunc("day", now() - interval '1 week')
            {% endif %}
            {% if not is_incremental() %}
            AND pu.minute >= '2022-4-1'
            {% endif %}
            --add in `pu.contract_address = sc.currency_address` in the future when ERC20 pairs are added in.
        LEFT JOIN {{ ref('nft_aggregators') }} agg
            ON (agg.contract_address = sc.call_from OR agg.contract_address = sc.router_caller) -- aggregator will either call pool directly or call the router
            AND agg.blockchain = 'ethereum'
        LEFT JOIN tokens_ethereum_nft tokens ON nft_contract_address = tokens.contract_address
    )

    ,swaps_exploded as (
        SELECT
            blockchain
            , project
            , version
            , block_date
            , block_time
            , block_number
            , explode(token_id) as token_id --nft.trades prefers each token id be its own row
            , token_standard
            , number_of_items/number_of_items as number_of_items
            , trade_type
            , trade_category
            , evt_type
            , seller
            , buyer
            , cast(amount_raw/number_of_items as double) as amount_raw
            , amount_original/number_of_items as amount_original
            , amount_usd/number_of_items as amount_usd
            , currency_symbol
            , currency_contract
            , project_contract_address
            , nft_contract_address
            , collection
            , tx_hash
            , tx_from
            , tx_to
            , aggregator_address
            , aggregator_name
            , platform_fee_amount/number_of_items as platform_fee_amount
            , cast(platform_fee_amount_raw/number_of_items as double) as platform_fee_amount_raw
            , platform_fee_amount_usd/number_of_items as platform_fee_amount_usd
            , platform_fee_percentage
            , pool_fee_amount/number_of_items as pool_fee_amount
            , pool_fee_amount_raw/number_of_items as pool_fee_amount_raw
            , pool_fee_amount_usd/number_of_items as pool_fee_amount_usd
            , pool_fee_percentage
            --below are null
            , royalty_fee_amount/number_of_items as royalty_fee_amount
            , royalty_fee_amount_raw/number_of_items as royalty_fee_amount_raw
            , royalty_fee_amount_usd/number_of_items as royalty_fee_amount_usd
            , royalty_fee_percentage
            , royalty_fee_currency_symbol
            , royalty_fee_receive_address
        FROM swaps_cleaned_w_metadata
    )

--final SELECT CTE
SELECT
    *
    , 'sudoswap-' || tx_hash || '-' || nft_contract_address || token_id::string || '-' || seller || '-' || amount_original::string || 'Trade' AS unique_trade_id
FROM swaps_exploded
