{{ config(
    schema = 'sudoswap_v2_arbitrum',

    alias = 'base_trades',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_number','tx_hash','sub_tx_trade_id'],
    post_hook='{{ expose_spells(\'["arbitrum"]\',
                            "project",
                            "sudoswap",
                            \'["ilemi"]\') }}'
    )
}}

WITH
    pools as (
        SELECT 
        *
        FROM {{ ref('sudoswap_v2_arbitrum_pool_creations')}}
    )

    , sell_nft_base as (
        SELECT
           'sell' as trade_category
            ,sp_start.call_trace_address_filled as swap_trace_address
            ,sp.call_trace_address as info_trace_address
            ,roy.call_trace_address as royalty_call_trace_address
            ,sp_start.tokenRecipient as trade_recipient
            ,sp.numItems 
            ,case when p.nft_type = 'ERC1155' then array[p.nft_id] else sp_start.nftIds end as token_ids
            ,sp.output_protocolFee/(sp.protocolFeeMultiplier/1e18) as amount_raw --for some reason sp.spotPrice is sometimes inaccurate for GDA curves? https://explorer.phalcon.xyz/tx/eth/0x20f4cf9aecae7d26ee170fbbf8017fb290bc6ce0caeae30ad2ae085d214d04d3
            ,sp.feeMultiplier
            ,sp.protocolFeeMultiplier
            ,sp.output_tradeFee
            ,sp.output_protocolFee
            ,COALESCE(case when cardinality(roy.output_1) = 0 then null else roy.output_1[1] end,cast(0 as uint256)) as royalty_fee_amount_raw
            ,cast(try(roy.output_0[1]) as varbinary) as royalty_fee_address
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
            FROM {{ source('sudoswap_v2_arbitrum','LSSVMPair_call_swapNFTsForToken') }}
            WHERE call_success
            {% if is_incremental() %}
            AND call_block_time >= date_trunc('day', now() - interval '7' day)
            {% endif %}
            ) sp_start
        LEFT JOIN (
            --each curve calculates info with all the data we need before a swap.
            SELECT * FROM {{ source('sudoswap_v2_arbitrum','ExponentialCurve_call_getSellInfo') }}
            UNION ALL 
            SELECT * FROM {{ source('sudoswap_v2_arbitrum','LinearCurve_call_getSellInfo') }}
            UNION ALL 
            SELECT * FROM {{ source('sudoswap_v2_arbitrum','XykCurve_call_getSellInfo') }}
            UNION ALL 
            SELECT * FROM {{ source('sudoswap_v2_arbitrum','GDACurve_call_getSellInfo') }}
        ) sp ON sp_start.call_tx_hash = sp.call_tx_hash
            AND sp_start.call_block_number = sp.call_block_number
            AND sp_start.call_trace_address_filled = slice(sp.call_trace_address,1,cardinality(sp_start.call_trace_address_filled))
            {% if is_incremental() %}
            AND sp.call_block_time >= date_trunc('day', now() - interval '7' day)
            {% endif %}
        --royalty is only called once per NFT contract, even if there are multiple token ids
        LEFT JOIN {{ source('sudoswap_v2_arbitrum','RoyaltyEngine_call_getRoyalty') }} roy 
            ON roy.call_tx_hash = sp.call_tx_hash
            AND roy.call_block_number = sp.call_block_number
            AND sp_start.call_trace_address_filled = slice(roy.call_trace_address,1,cardinality(sp_start.call_trace_address_filled))
            AND cardinality(roy.output_0) is not null --ignore if no royalty returned.
            {% if is_incremental() %}
            AND roy.call_block_time >= date_trunc('day', now() - interval '7' day)
            {% endif %}
        LEFT JOIN pools p ON p.pool_address = sp_start.contract_address
    )
    
    , buy_nft_base as (
        SELECT
           'buy' as trade_category
            ,sp_start.call_trace_address_filled as swap_trace_address
            ,sp.call_trace_address as info_trace_address
            ,roy.call_trace_address as royalty_call_trace_address
            ,sp_start.nftRecipient as trade_recipient
            ,sp.numItems 
            ,case when p.nft_type = 'ERC1155' then array[p.nft_id] else sp_start.nftIds end as token_ids
            ,sp.output_protocolFee/(sp.protocolFeeMultiplier/1e18) as amount_raw
            ,sp.feeMultiplier
            ,sp.protocolFeeMultiplier
            ,sp.output_tradeFee
            ,sp.output_protocolFee
            ,COALESCE(case when cardinality(roy.output_1) = 0 then null else roy.output_1[1] end,cast(0 as uint256)) as royalty_fee_amount_raw
            ,cast(try(roy.output_0[1]) as varbinary) as royalty_fee_address
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
            FROM {{ source('sudoswap_v2_arbitrum','LSSVMPair_call_swapTokenForSpecificNFTs') }}
            WHERE call_success
            {% if is_incremental() %}
            AND call_block_time >= date_trunc('day', now() - interval '7' day)
            {% endif %}
            ) sp_start
        LEFT JOIN (
            SELECT * FROM {{ source('sudoswap_v2_arbitrum','ExponentialCurve_call_getBuyInfo') }}
            UNION ALL 
            SELECT * FROM {{ source('sudoswap_v2_arbitrum','LinearCurve_call_getBuyInfo') }}
            UNION ALL 
            SELECT * FROM {{ source('sudoswap_v2_arbitrum','XykCurve_call_getBuyInfo') }}
            UNION ALL 
            SELECT * FROM {{ source('sudoswap_v2_arbitrum','GDACurve_call_getBuyInfo') }}
        ) sp ON sp_start.call_tx_hash = sp.call_tx_hash
            AND sp_start.call_block_number = sp.call_block_number
            AND sp_start.call_trace_address_filled = slice(sp.call_trace_address,1,cardinality(sp_start.call_trace_address_filled))
            {% if is_incremental() %}
            AND sp.call_block_time >= date_trunc('day', now() - interval '7' day)
            {% endif %}
        LEFT JOIN {{ source('sudoswap_v2_arbitrum','RoyaltyEngine_call_getRoyalty') }} roy 
            ON roy.call_tx_hash = sp.call_tx_hash
            AND roy.call_block_number = sp.call_block_number
            AND sp_start.call_trace_address_filled = slice(roy.call_trace_address,1,cardinality(sp_start.call_trace_address_filled))
            {% if is_incremental() %}
            AND roy.call_block_time >= date_trunc('day', now() - interval '7' day)
            {% endif %}
        LEFT JOIN pools p ON p.pool_address = sp_start.contract_address
    )
    
    , trades as (
        SELECT
            'arbitrum' as blockchain
            , 'sudoswap' as project
            , 'v2' as project_version
            , call_block_time as block_time
            , cast(date_trunc('day', call_block_time) as date) as block_date
            , cast(date_trunc('month', call_block_time) as date) as block_month
            , call_block_number as block_number
            , call_tx_hash as tx_hash
            , pool_address as project_contract_address
            , case when trade_category = 'sell' then pool_address else trade_recipient end as buyer
            , case when trade_category = 'buy' then pool_address else trade_recipient end as seller
            , nft_contract_address
            , one_nft_token_id as nft_token_id
            , case when nft_type = 'ERC721' then 1 else numItems end as nft_amount
            , case when numItems > 0 then 'multiple' else 'single' end as trade_type
            , trade_category
            , token_contract_address as currency_contract
            , cast(case when nft_type = 'ERC721' then amount_raw/numItems else amount_raw end as uint256) as price_raw
            , cast(case when nft_type = 'ERC721' then output_protocolFee/numItems else output_protocolFee end as uint256) as platform_fee_amount_raw
            , cast(case when nft_type = 'ERC721' then output_tradeFee/numItems else output_tradeFee end as uint256) as pool_fee_amount_raw
            , cast(case when nft_type = 'ERC721' then royalty_fee_amount_raw/numItems else royalty_fee_amount_raw end as uint256) as royalty_fee_amount_raw
            , 0x6132912d8009268dcc457c003a621a0de405dbe0 as platform_fee_address --factory recieves the fees
            , royalty_fee_address
            , row_number() over (partition by call_tx_hash order by one_nft_token_id) as sub_tx_trade_id
            , nft_type
            , cast(amount_raw/numItems as uint256) as spot_price_raw
        FROM (
            SELECT * FROM sell_nft_base
            UNION ALL 
            SELECT * FROM buy_nft_base
        ) tr
        LEFT JOIN unnest(token_ids) as t(one_nft_token_id) ON TRUE
    )

    , trades_final as (
        SELECT 
        blockchain
        , project
        , project_version
        , block_time
        , block_date
        , block_month
        , block_number
        , tx_hash
        , project_contract_address
        , buyer
        , seller
        , nft_contract_address
        , nft_token_id
        , nft_amount
        , trade_type
        , trade_category
        , currency_contract
        , price_raw + platform_fee_amount_raw + pool_fee_amount_raw + royalty_fee_amount_raw as price_raw
        , platform_fee_amount_raw
        , pool_fee_amount_raw
        , royalty_fee_amount_raw
        , platform_fee_address
        , royalty_fee_address
        , sub_tx_trade_id
        , nft_type
        , spot_price_raw
        FROM trades
    )

{{ add_nft_tx_data('trades_final', 'arbitrum') }}