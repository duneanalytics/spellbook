{{ config(
    schema = 'sudoswap_v2_ethereum',

    alias = 'base_trades',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_number','tx_hash','sub_tx_trade_id'],
    post_hook='{{ expose_spells(\'["ethereum"]\',
                            "project",
                            "sudoswap",
                            \'["ilemi"]\') }}'
    )
}}

WITH
    pools as (
        SELECT 
        *
        FROM {{ ref('sudoswap_v2_ethereum_pool_creations')}}
    )

        , sell_nft_base as (
        SELECT
           'sell' as trade_category
            ,sp_start.call_trace_address_filled as swap_trace_address
            ,sp.call_trace_address as info_trace_address
            ,roy.call_trace_address as royalty_call_trace_address
            ,sp.numItems 
            ,sp_start.nftIds as token_ids
            ,sp.output_protocolFee/(sp.protocolFeeMultiplier/1e18) as spotPrice --for some reason sp.spotPrice is sometimes inaccurate for GDA curves? https://explorer.phalcon.xyz/tx/eth/0x20f4cf9aecae7d26ee170fbbf8017fb290bc6ce0caeae30ad2ae085d214d04d3
            ,sp.feeMultiplier
            ,sp.protocolFeeMultiplier
            ,sp.output_tradeFee
            ,sp.output_protocolFee
            ,COALESCE(case when cardinality(roy.output_1) = 0 then null else roy.output_1[1] end,cast(0 as uint256)) as royalty_fee_amount_raw
            ,p.token_contract_address
            ,p.nft_type
            ,p.nft_contract_address
            ,p.pool_type
            ,p.bonding_curve
            ,p.pool_address
            ,sp.call_tx_hash
            ,sp.call_block_time
            ,sp.call_block_number
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
    )
    
    , buy_nft_base as (
        SELECT
           'buy' as trade_category
            ,sp_start.call_trace_address_filled as swap_trace_address
            ,sp.call_trace_address as info_trace_address
            ,roy.call_trace_address as royalty_call_trace_address
            ,sp.numItems 
            ,sp_start.nftIds as token_ids
            ,sp.output_protocolFee/(sp.protocolFeeMultiplier/1e18) as spotPrice
            ,sp.feeMultiplier
            ,sp.protocolFeeMultiplier
            ,sp.output_tradeFee
            ,sp.output_protocolFee
            ,COALESCE(case when cardinality(roy.output_1) = 0 then null else roy.output_1[1] end,cast(0 as uint256)) as royalty_fee_amount_raw
            ,p.token_contract_address
            ,p.nft_type
            ,p.nft_contract_address
            ,p.pool_type
            ,p.bonding_curve
            ,p.pool_address
            ,sp.call_tx_hash
            ,sp.call_block_time
            ,sp.call_block_number
        FROM (
            SELECT 
                *
                , case when cardinality(call_trace_address) = 0 then array[0] else call_trace_address end as call_trace_address_filled 
            FROM {{ source('sudoswap_v2_ethereum','LSSVMPair_call_swapTokenForSpecificNFTs') }}
            ) sp_start
        LEFT JOIN (
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
        LEFT JOIN {{ source('sudoswap_v2_ethereum','RoyaltyEngine_call_getRoyalty') }} roy 
            ON roy.call_tx_hash = sp.call_tx_hash
            AND roy.call_block_number = sp.call_block_number
            AND sp_start.call_trace_address_filled = slice(roy.call_trace_address,1,cardinality(sp_start.call_trace_address_filled))
        LEFT JOIN pools p ON p.pool_address = sp_start.contract_address
    )
    
    , trades as (
        SELECT 
            trade_category
            , numItems/cardinality(token_ids) as number_of_items_unnested
            , token_ids
            , spotPrice as amount_raw
            , output_tradeFee as trade_fee_amount_raw
            , output_protocolFee as protocol_fee_amount_raw
            , royalty_fee_amount_raw
            , token_contract_address
            , nft_contract_address
            , nft_type
            , pool_type
            , bonding_curve
            , pool_address
            , call_block_time as block_time
            , call_block_number as block_number
            , call_tx_hash as tx_hash
        FROM (
            SELECT * FROM sell_nft_base
            UNION ALL 
            SELECT * FROM buy_nft_base
        ) tr
    )

SELECT
    'ethereum' as blockchain
    , 'sudoswap' as project
    , 'v2' as version
    , block_time
    , block_number
    , tx_hash
    , pool_address as project_contract_address
    , case when trade_category = 'sell' then pool_address else tx_from end as buyer
    , case when trade_category = 'buy' then pool_address else tx_from end as seller
    , nft_contract_address
    , one_nft_token_id as token_id
    , number_of_items_unnested as nft_amount
    , case when number_of_items > 0 then 'multiple' else 'single' end as trade_type
    , trade_category
    , token_contract_address as currency_contract
    , cast(amount_original/number_of_items_unnested as uint256) as price_raw
    , cast(platform_fee_amount_raw/number_of_items_unnested as uint256) as platform_fee_amount_raw
    , cast(pool_fee_amount_raw/number_of_items_unnested as uint256) as pool_fee_amount_raw
    , cast(royalty_fee_amount_raw/number_of_items_unnested as uint256) as royalty_fee_amount_raw
    , cast(null as varbinary) as platform_fee_address --come back and fill this in later
    , cast(null as varbinary) as royalty_fee_address
    , row_number() over (partition by tx_hash order by one_nft_token_id) as sub_tx_trade_id
FROM trades t
LEFT JOIN unnest(token_ids) as t(one_nft_token_id) ON TRUE
