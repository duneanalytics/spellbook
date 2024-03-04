{{ config(
    schema = 'sudoswap_v2_ethereum',

    alias = 'base_trades',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_number','tx_hash','sub_tx_trade_id'],
    )
}}
--base table CTEs
WITH
    pools as (
        SELECT 
             output_pair AS pool_address,
             nft_contract_address,
             nft_type,
             case when token = 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2 then 'ETH' else 'ERC20' end as token_type,
             token,
             CASE
                WHEN bonding_curve = 0xe5d78fec1a7f42d2F3620238C498F088A866FdC5 THEN 'linear'
                WHEN bonding_curve = 0xfa056C602aD0C0C4EE4385b3233f2Cb06730334a THEN 'exponential'
                WHEN bonding_curve = 0xc7fB91B6cd3C67E02EC08013CEBb29b1241f3De5 THEN 'xyk'
                WHEN bonding_curve = 0x1fD5876d4A3860Eb0159055a3b7Cb79fdFFf6B67 then 'GDA'
                ELSE 'other'
              END as bonding_curve_type,
              CASE
                WHEN pool_type_raw = 0 THEN 'token'
                WHEN pool_type_raw = 1 THEN 'nft'
                WHEN pool_type_raw = 2 THEN 'trade'
              END AS pool_type,
              contract_address as pool_factory,
              call_block_time,
              call_tx_hash
        FROM (
             SELECT 
                  output_pair,
                  _nft AS nft_contract_address,
                --   _assetRecipient,
                  _bondingCurve as bonding_curve,
                  _poolType as pool_type_raw,
                  'ERC721' as nft_type,
                  0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2 as token,
                  contract_address,
                  call_block_time,
                  call_tx_hash
            FROM {{ source('sudoswap_v2_ethereum','LSSVMPairFactory_call_createPairERC721ETH') }}
            UNION ALL
            SELECT
                  output_pair,
                  _nft AS nft_contract_address,
                --   _assetRecipient,
                  _bondingCurve as bonding_curve,
                  _poolType as pool_type_raw,
                  'ERC1155' as nft_type,
                  0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2 as token,
                  contract_address,
                  call_block_time,
                  call_tx_hash
            FROM {{ source('sudoswap_v2_ethereum','LSSVMPairFactory_call_createPairERC1155ETH') }}
            UNION ALL 
            SELECT 
                output_pair
                , from_hex(json_extract_scalar(params,'$.nft')) as nft_contract_address
                , from_hex(json_extract_scalar(params,'$.bondingCurve')) as bonding_curve
                , cast(json_extract_scalar(params,'$.poolType') as int) as pool_type_raw
                , 'ERC1155' as nft_type
                , from_hex(json_extract_scalar(params,'$.token')) as token_type
                , contract_address
                , call_block_time
                , call_tx_hash
            FROM {{ source('sudoswap_v2_ethereum','LSSVMPairFactory_call_createPairERC1155ERC20') }}
            UNION ALL 
            SELECT 
                output_pair
                , from_hex(json_extract_scalar(params,'$.nft')) as nft_contract_address
                , from_hex(json_extract_scalar(params,'$.bondingCurve')) as bonding_curve
                , cast(json_extract_scalar(params,'$.poolType') as int) as pool_type_raw
                , 'ERC721' as nft_type
                , from_hex(json_extract_scalar(params,'$.token')) as token_type
                , contract_address
                , call_block_time
                , call_tx_hash
            FROM {{ source('sudoswap_v2_ethereum','LSSVMPairFactory_call_createPairERC721ERC20') }}
        )
        -- WHERE output_pair = 0xcdb8f114d2fb28a4b85bb1ab6e09444006ef5385
    )

        , sell_nft_base as (
        SELECT
           'sell' as trade_category
            ,sp_start.call_trace_address_filled as swap_trace_address
            ,sp.call_trace_address as info_trace_address
            ,roy.call_trace_address as royalty_call_trace_address
            ,sp.numItems 
            ,sp_start.nftIds
            ,sp.output_protocolFee/(sp.protocolFeeMultiplier/1e18) as spotPrice --sp.spotPrice --for some reason spot price is sometimes inaccurate? https://explorer.phalcon.xyz/tx/eth/0x20f4cf9aecae7d26ee170fbbf8017fb290bc6ce0caeae30ad2ae085d214d04d3
            ,sp.feeMultiplier
            ,sp.protocolFeeMultiplier
            ,sp.output_tradeFee
            ,sp.output_protocolFee
            ,COALESCE(case when cardinality(roy.output_1) = 0 then null else roy.output_1[1] end,cast(0 as uint256)) as royalty_fee_amount_raw
            ,p.token
            ,p.nft_type
            ,p.nft_contract_address
            ,p.pool_type
            ,p.bonding_curve_type
            ,p.pool_address
            ,sp.call_tx_hash
            ,sp.call_block_time
        FROM (
            SELECT 
                *
                --need this for a clean join below on trace_address, since top level calls are empty for trace address
                , case when cardinality(call_trace_address) = 0 then array[0] else call_trace_address end as call_trace_address_filled 
            FROM {{ source('sudoswap_v2_ethereum','LSSVMPair_call_swapNFTsForToken') }}
            ) sp_start
        LEFT JOIN (
            --each curve calculates info with all the data we need before a swap.
            SELECT * FROM {{ source('sudoswap_v2_ethereum','ExponentialCurve_call_getSellInfo') }}
            UNION ALL 
            SELECT * FROM {{ source('sudoswap_v2_ethereum','LinearCurve_call_getSellInfo') }}
            UNION ALL 
            SELECT * FROM {{ source('sudoswap_v2_ethereum','XykCurve_call_getSellInfo') }}
            UNION ALL 
            SELECT * FROM {{ source('sudoswap_v2_ethereum','GDACurve_call_getSellInfo') }}
        ) sp ON sp_start.call_tx_hash = sp.call_tx_hash
            AND sp_start.call_block_number = sp.call_block_number
            AND sp_start.call_trace_address_filled = slice(sp.call_trace_address,1,cardinality(sp_start.call_trace_address_filled))
        --royalty is only called once per NFT contract, even if there are multiple token ids
        LEFT JOIN {{ source('sudoswap_v2_ethereum','RoyaltyEngine_call_getRoyalty') }} roy 
            ON roy.call_tx_hash = sp.call_tx_hash
            AND roy.call_block_number = sp.call_block_number
            AND sp_start.call_trace_address_filled = slice(roy.call_trace_address,1,cardinality(sp_start.call_trace_address_filled))
            AND cardinality(roy.output_0) is not null --ignore if no royalty returned.
        LEFT JOIN pools p ON p.pool_address = sp_start.contract_address
        -- WHERE sp.contract_address = 0xcdb8f114d2fb28a4b85bb1ab6e09444006ef5385
        -- WHERE sp_start.call_tx_hash = 0x20f4cf9aecae7d26ee170fbbf8017fb290bc6ce0caeae30ad2ae085d214d04d3
    )
    
    , buy_nft_base as (
        SELECT
           'buy' as trade_category
            ,sp_start.call_trace_address_filled as swap_trace_address
            ,sp.call_trace_address as info_trace_address
            ,roy.call_trace_address as royalty_call_trace_address
            ,sp.numItems 
            ,sp_start.nftIds
            ,sp.output_protocolFee/(sp.protocolFeeMultiplier/1e18) as spotPrice
            ,sp.feeMultiplier
            ,sp.protocolFeeMultiplier
            ,sp.output_tradeFee
            ,sp.output_protocolFee
            ,COALESCE(case when cardinality(roy.output_1) = 0 then null else roy.output_1[1] end,cast(0 as uint256)) as royalty_fee_amount_raw
            ,p.token
            ,p.nft_type
            ,p.nft_contract_address
            ,p.pool_type
            ,p.bonding_curve_type
            ,p.pool_address
            ,sp.call_tx_hash
            ,sp.call_block_time
        FROM (
            SELECT 
                *
                --need this for a clean join below on trace_address, since top level calls are empty for trace address
                , case when cardinality(call_trace_address) = 0 then array[0] else call_trace_address end as call_trace_address_filled 
            FROM {{ source('sudoswap_v2_ethereum','LSSVMPair_call_swapTokenForSpecificNFTs') }}
            ) sp_start
        LEFT JOIN (
            --each curve calculates info with all the data we need before a swap.
            SELECT * FROM {{ source('sudoswap_v2_ethereum','ExponentialCurve_call_getSellInfo') }}
            UNION ALL 
            SELECT * FROM {{ source('sudoswap_v2_ethereum','LinearCurve_call_getSellInfo') }}
            UNION ALL 
            SELECT * FROM {{ source('sudoswap_v2_ethereum','XykCurve_call_getSellInfo') }}
            UNION ALL 
            SELECT * FROM {{ source('sudoswap_v2_ethereum','GDACurve_call_getSellInfo') }}
        ) sp ON sp_start.call_tx_hash = sp.call_tx_hash
            AND sp_start.call_block_number = sp.call_block_number
            AND sp_start.call_trace_address_filled = slice(sp.call_trace_address,1,cardinality(sp_start.call_trace_address_filled))
        --royalty is only called once per NFT contract, even if there are multiple token ids
        LEFT JOIN {{ source('sudoswap_v2_ethereum','RoyaltyEngine_call_getRoyalty') }} roy 
            ON roy.call_tx_hash = sp.call_tx_hash
            AND roy.call_block_number = sp.call_block_number
            AND sp_start.call_trace_address_filled = slice(roy.call_trace_address,1,cardinality(sp_start.call_trace_address_filled))
        LEFT JOIN pools p ON p.pool_address = sp_start.contract_address
        -- WHERE sp.contract_address = 0xcdb8f114d2fb28a4b85bb1ab6e09444006ef5385
    )
    
    , trades as (
        SELECT 
            trade_category
            , numItems as number_of_items
            , nftIds
            , spotPrice as amount_raw
            , spotPrice/pow(10,COALESCE(tk.decimals,18)) as amount_original
            , spotPrice/pow(10,COALESCE(tk.decimals,18))*p.price as amount_usd
            , output_tradeFee as trade_fee_amount_raw
            , output_tradeFee/pow(10,COALESCE(tk.decimals,18)) as trade_fee
            , output_tradeFee/pow(10,COALESCE(tk.decimals,18))*p.price as trade_fee_amount_usd
            , feeMultiplier/1e18 as trade_fee_percentage
            , output_protocolFee as protocol_fee_amount_raw
            , output_protocolFee/pow(10,COALESCE(tk.decimals,18)) as protocol_fee
            , output_protocolFee/pow(10,COALESCE(tk.decimals,18))*p.price as protocol_fee_amount_usd
            , protocolFeeMultiplier/1e18 as protocol_fee_percentage
            , royalty_fee_amount_raw
            , royalty_fee_amount_raw/pow(10,COALESCE(tk.decimals,18)) as royalty_fee
            , royalty_fee_amount_raw/pow(10,COALESCE(tk.decimals,18))*p.price as royalty_fee_amount_usd
            , case when spotPrice = 0 then 0 else cast(royalty_fee_amount_raw as double)/cast(spotPrice as double) end as royalty_fee_percentage
            , token
            , tk.symbol as token_symbol
            , nft_contract_address
            , nft.name as nft_name
            , nft_type
            , pool_type
            , bonding_curve_type
            , pool_address
            , call_block_time as block_time
            , call_tx_hash as tx_hash
            , tx."from" as tx_from
            , tx.to as tx_to
        FROM (
            SELECT * FROM sell_nft_base
            UNION ALL 
            SELECT * FROM buy_nft_base
        ) tr
        LEFT JOIN tokens.erc20 tk ON tk.contract_address = tr.token AND tk.blockchain = 'ethereum'
        LEFT JOIN tokens.nft nft ON nft.contract_address = tr.nft_contract_address AND nft.blockchain = 'ethereum'
        LEFT JOIN prices.usd p ON p.minute = date_trunc('minute',tr.call_block_time) AND p.blockchain = 'ethereum' AND p.contract_address = tr.token
        LEFT JOIN ethereum.transactions tx ON tx.hash = tr.call_tx_hash
    )

-- unnest nftIds, also need to unnest erc1155 by amount.

-- SELECT
--      'ethereum' as blockchain
--     , 'sudoswap' as project
--     , 'v2' as project_version
--     , block_time
--     , block_number
--     , tx_hash
--     , project_contract_address
--     , buyer
--     , seller
--     , nft_contract_address
--     , one_nft_token_id as nft_token_id --nft.trades prefers each token id be its own row
--     , uint256 '1' as nft_amount
--     , trade_type
--     , trade_category
--     , currency_contract
--     , cast(price_raw/number_of_items as uint256) as price_raw
--     , cast(platform_fee_amount_raw/number_of_items as uint256) as platform_fee_amount_raw
--     , uint256 '0' as royalty_fee_amount_raw
--     , cast(pool_fee_amount_raw/number_of_items as uint256) as pool_fee_amount_raw
--     , protocolfee_recipient as platform_fee_address
--     , cast(null as varbinary) as royalty_fee_address
--     , row_number() over (partition by tx_hash order by one_nft_token_id) as sub_tx_trade_id
-- FROM trades
-- CROSS JOIN UNNEST(nft_token_id) as foo(one_nft_token_id)

