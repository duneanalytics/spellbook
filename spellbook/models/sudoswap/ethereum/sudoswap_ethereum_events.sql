{{ config(
        alias = 'events',
        partition_by = ['block_date'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['block_date', 'unique_trade_id']
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
        FROM {{ source('sudo_amm_ethereum','LSSVMPairFactory_call_createPairETH') }}
        WHERE call_success
    ),

    swaps as (
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
                , tokenRecipient as call_from
                , 'Sell' as trade_category 
            FROM {{ source('sudo_amm_ethereum','LSSVMPair_general_call_swapNFTsForToken') }}
            WHERE call_success = true
            {% if is_incremental() %}
            -- this filter will only be applied on an incremental run. We only want to update with new swaps.
            AND call_block_time >= (select max(block_time) from {{ this }})
            {% endif %}

            UNION ALL
            SELECT 
                contract_address
                , call_tx_hash
                , call_trace_address
                , call_block_time
                , call_block_number
                , call_success
                , nftRecipient as call_from
                , 'Buy' as trade_category 
            FROM {{ source('sudo_amm_ethereum','LSSVMPair_general_call_swapTokenForAnyNFTs') }}
            WHERE call_success = true
            {% if is_incremental() %}
            AND call_block_time >= (select max(block_time) from {{ this }})
            {% endif %}

            UNION ALL
            SELECT
                contract_address
                , call_tx_hash
                , call_trace_address
                , call_block_time
                , call_block_number
                , call_success
                , nftRecipient as call_from
                , 'Buy' as trade_category 
            FROM {{ source('sudo_amm_ethereum','LSSVMPair_general_call_swapTokenForSpecificNFTs') }}
            WHERE call_success = true
            {% if is_incremental() %}
            AND call_block_time >= (select max(block_time) from {{ this }})
            {% endif %}
        ) s
    ),

    owner_fee_update as (
        SELECT 
            *
        FROM {{ source('sudo_amm_ethereum','LSSVMPair_general_evt_FeeUpdate') }}
    ),

    protocol_fee_update as (
        SELECT
            *
        FROM {{ source('sudo_amm_ethereum','LSSVMPairFactory_evt_ProtocolFeeMultiplierUpdate') }}
    ),

    tokens_ethereum_nft as (
        SELECT 
            *
        FROM {{ ref('tokens_ethereum_nft') }}
    ),

    nft_ethereum_aggregators as (
        SELECT 
            *
        FROM {{ ref('nft_ethereum_aggregators') }}
    ),

--logic CTEs
    swaps_w_fees as (
        SELECT 
            *
        FROM (
            SELECT
                call_tx_hash
                , call_block_time
                , call_block_number
                , contract_address as pair_address
                , call_trace_address
                , ownerfee 
                , protocolfee 
                , trade_category
                , nftcontractaddress
                , asset_recip
                , call_from
                , row_number() OVER (partition by call_tx_hash, contract_address, call_trace_address order by fee_update_time desc, protocolfee_updatetime desc) as ordering
            FROM (
                SELECT 
                    swaps.*
                    , COALESCE(fu.newfee, pc.initialfee)/1e18 as ownerfee --most recent ownerfee, depends on bonding curve to implement it correctly. See explanation in fee table schema.
                    , COALESCE(fu.evt_block_time, pc.block_time) as fee_update_time
                    , pfu.newMultiplier/1e18 as protocolfee --most recent protocolfee, depends on bonding curve to implement it correctly. See explanation in fee table schema.
                    , pfu.evt_block_time as protocolfee_updatetime
                    , pc.nftcontractaddress
                    , pc.asset_recip
                FROM swaps
                JOIN pairs_created pc ON pc.pair_address = contract_address --remember swaps from other NFT addresses won't appear!
                LEFT JOIN owner_fee_update fu ON swaps.call_block_time >= fu.evt_block_time AND swaps.contract_address = fu.contract_address
                LEFT JOIN protocol_fee_update pfu ON swaps.call_block_time >= pfu.evt_block_time
            ) a
        ) b
        WHERE ordering = 1 --we want to keep the most recent ownerfee and protocol fee for each individual call (trade)
    ),

    swaps_w_traces as (
        -- we traces to get NFT and ETH transfer data because sudoswap doesn't emit any data in events for swaps, so we have to piece it together manually based on trace_address.
        SELECT
            sb.call_block_time
            , sb.call_block_number
            , sb.trade_category
            , CASE WHEN sb.trade_category = 'Buy' THEN SUM(value)/(1+sb.ownerfee+sb.protocolfee)
                ELSE SUM(value)/(1-sb.ownerfee-sb.protocolfee)
                END as base_price
            , SUM(value) as trade_price_eth --should give total value of the trade (buy or sell)
            , ARRAY_AGG(distinct CASE WHEN substring(input,1,10)='0x42842e0e' THEN bytea2numeric_v2(substring(input,139,64))::int ELSE null END) 
                as token_id
            , sb.call_tx_hash
            , sb.call_from
            , sb.pair_address
            , sb.nftcontractaddress
            , sb.ownerfee
            , sb.protocolfee
        FROM swaps_w_fees sb 
        LEFT JOIN {{ source('ethereum', 'traces') }} tr 
            ON tr.type = 'call'
            AND tr.call_type = 'call'
            AND tr.tx_hash = sb.call_tx_hash
            AND (
                (cardinality(call_trace_address) != 0 AND call_trace_address = slice(tr.trace_address,1,cardinality(call_trace_address))) --either a normal tx where trace address helps us narrow down which subtraces to look at for ETH transfers or NFT transfers.
                OR cardinality(call_trace_address) = 0 --or a private tx, in which case we assume its a single swap to the router (like 0x34a52a94fce15c090cc16adbd6824948c731ecb19a39350633590a9cd163658b). if multiple swaps happen in a private tx we will need a different workaround (as values will duplicate)
                )
            AND tr.to != '0xb16c1342e617a5b6e4b631eb114483fdb289c0a4' --we don't want duplicates from protocol fee transfer to show up in table. This needs to be most up to date funding recipient in the future, but should just be pair address for now.
            AND tr.to != asset_recip --we don't want duplicates where eth is transferred to asset recipient instead of pool in the case it isn't a trade pool
            {% if is_incremental() %}
            AND tr.block_time >= (select max(block_time) from {{ this }})
            {% endif %}
            {% if not is_incremental() %}
            AND tr.block_time >= '2022-4-1'
            {% endif %}
        GROUP BY 1,2,3,7,8,9,10,11,12
    ),

    swaps_cleaned as (
        --formatting swaps for sudoswap_ethereum_events defined schema
        SELECT 
            'ethereum' as blockchain 
            , 'sudoswap' as project 
            , 'v1' as version
            , TRY_CAST(date_trunc('DAY', call_block_time) AS date) AS block_date
            , call_block_time as block_time 
            , call_block_number as block_number
            , token_id
            , 'ERC721' as token_standard
            , cardinality(token_id) as number_of_items
            , CASE WHEN cardinality(token_id) > 1 THEN 'Bundle Trade'
                ELSE 'Single Item Trade' 
               END as trade_type
            , trade_category
            , 'Trade' as evt_type
            , CASE WHEN trade_category = 'Buy' THEN pair_address --AMM is selling if an NFT is being bought
                ELSE call_from
                END as seller 
            , CASE WHEN trade_category = 'Sell' THEN pair_address --AMM is buying if an NFT is being sold
                ELSE call_from
                END as buyer
            , trade_price_eth as amount_raw
            , trade_price_eth/1e18 as amount_original
            , 'ETH' as currency_symbol
            , '0x0000000000000000000000000000000000000000' as currency_contract --ETH
            , nftcontractaddress as nft_contract_address
            , '0xb16c1342e617a5b6e4b631eb114483fdb289c0a4' as project_contract_address --not sure what this is? I put their main factory for now
            , call_tx_hash as tx_hash
            , '' as evt_index --we didn't use events in our case for decoding, so this will be null until we find a way to tie it together.
            , base_price*protocolfee as platform_fee_amount_raw
            , (base_price*protocolfee)/1e18 as platform_fee_amount
            , protocolfee as platform_fee_percentage
            --royalties don't technically exist on AMM, but there are owner fees for the pool that can be treated as royalties in the future.
            , base_price*ownerfee as royalty_fee_amount_raw
            , (base_price*ownerfee)/1e18 as royalty_fee_amount
            , ownerfee as royalty_fee_percentage
            , pair_address as royalty_fee_receive_address
            , 'ETH' as royalty_fee_currency_symbol
        FROM swaps_w_traces
    ),

    swaps_cleaned_w_metadata as (
        SELECT 
            sc.*
            , tokens.name AS collection
            , agg.name as aggregator_name
            , agg.contract_address as aggregator_address
            , sc.amount_original*pu.price as amount_usd 
            , sc.royalty_fee_amount*pu.price as royalty_fee_amount_usd
            , sc.platform_fee_amount*pu.price as platform_fee_amount_usd
            , tx.from as tx_from
            , tx.to as tx_to
            , 'sudoswap-' || sc.tx_hash || '-' || sc.nft_contract_address || sc.token_id::string || '-' || sc.seller || '-' || sc.amount_original::string || 'Trade' AS unique_trade_id
        FROM swaps_cleaned sc
        LEFT JOIN {{ source('prices', 'usd') }} pu ON pu.blockchain='ethereum'
            AND date_trunc('minute', pu.minute)=date_trunc('minute', sc.block_time)
            AND symbol = 'WETH'
            {% if is_incremental() %}
            AND pu.minute >= (select max(block_time) from {{ this }})
            {% endif %}
            {% if not is_incremental() %}
            AND pu.minute >= '2022-4-1'
            {% endif %}
            --add in `pu.contract_address = sc.currency_address` in the future when ERC20 pairs are added in.
        LEFT JOIN {{ source('ethereum', 'transactions') }} tx 
            ON tx.hash=sc.tx_hash 
            {% if is_incremental() %}
            AND tx.block_time >= (select max(block_time) from {{ this }})
            {% endif %}
            {% if not is_incremental() %}
            AND tx.block_time >= '2022-4-1'
            {% endif %}
        LEFT JOIN nft_ethereum_aggregators agg ON agg.contract_address = tx.to --assumes aggregator is the top level call. Will need to change this to check for agg calls in internal traces later on.
        LEFT JOIN tokens_ethereum_nft tokens ON nft_contract_address = tokens.contract_address
    )

--final SELECT CTE
SELECT * FROM swaps_cleaned_w_metadata