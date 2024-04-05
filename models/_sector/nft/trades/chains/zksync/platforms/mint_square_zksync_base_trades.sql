{{ config(
    schema = 'mint_square_zksync',
    aliAS = 'base_trades',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_number','tx_hash','sub_tx_trade_id'],
    )
}}

WITH mintsquare_trades AS (
    SELECT *
    , ROW_NUMBER() OVER (PARTITION BY tx_hash, nft_contract_address, nft_token_id ORDER BY evt_index ASC) AS id
    FROM (
        SELECT 
            ta.evt_block_time AS block_time
            , ta.tokenId AS nft_token_id
            , ta.amount AS nft_amount
            , 'Buy' AS trade_category
            , ta.maker AS seller
            , ta.taker AS buyer
            , ta.price AS price_raw
            , ta.currency AS currency_contract
            , ta.collection AS nft_contract_address
            , ta.contract_address AS project_contract_address
            , ta.evt_tx_hash AS tx_hash
            , ta.evt_block_number AS block_number
            , ta.evt_index
            , ta.strategy
        FROM {{ source('mint_square_zksync','Marketplace_evt_TakerAsk') }} ta
        {% if is_incremental() %}
        WHERE ta.{{incremental_predicate('evt_block_time')}}
        {% endif %}

        UNION ALL

        SELECT 
            tb.evt_block_time AS block_time
            , tb.tokenId AS nft_token_id
            , tb.amount AS nft_amount
            , 'Offer Accepted' AS trade_category
            , tb.maker AS seller
            , tb.taker AS buyer
            , tb.price AS price_raw
            , tb.currency AS currency_contract
            , tb.collection AS nft_contract_address
            , tb.contract_address AS project_contract_address
            , tb.evt_tx_hash AS tx_hash
            , tb.evt_block_number AS block_number
            , tb.evt_index
            , tb.strategy
        FROM {{ source('mint_square_zksync','Marketplace_evt_TakerBid') }} tb
        {% if is_incremental() %}
        WHERE tb.{{incremental_predicate('evt_block_time')}}
        {% endif %}
        )
    )

, royalties AS (
    SELECT 
        evt_block_time AS block_time
        , evt_tx_hash AS tx_hash
        , evt_index
        , collection AS nft_contract_address
        , tokenId AS nft_token_id
        , amount
        , royaltyRecipient
        , ROW_NUMBER() OVER (PARTITION BY evt_tx_hash, collection, tokenId ORDER BY evt_index ASC) AS id
    FROM {{ source('mint_square_zksync','Marketplace_evt_RoyaltyPayment') }}
    {% if is_incremental() %}
    WHERE {{incremental_predicate('evt_block_time')}}
    {% endif %}
    )

, platform_fees AS (
    SELECT 
        DISTINCT contract_address
        , CAST(output_0 AS double)/100 AS fee_percentage
    FROM {{ source('mint_square_zksync','StrategyHighestBidderAuctionSale_call_viewProtocolFee') }}
    UNION ALL
    SELECT 
        DISTINCT contract_address
        , CAST(output_0 AS double)/100 AS fee_percentage
    FROM {{ source('mint_square_zksync','StrategyStandardSaleForFixedPrice_call_viewProtocolFee') }}
    UNION ALL
    SELECT 
        DISTINCT contract_address
        , CAST(output_0 AS double)/100 AS fee_percentage
    FROM {{ source('mint_square_zksync','StrategyAnyItemFromCollectionForFixedPrice_call_viewProtocolFee') }}
    )

, base_trades AS (
    SELECT
        'zksync' AS blockchain
        , 'mintsquare' AS project
        , 'v1' AS project_version
        , m.block_time
        , CAST(date_trunc('day', m.block_time) AS date) AS block_date
        , CAST(date_trunc('month', m.block_time) AS date) AS block_month
        , m.block_number
        , m.tx_hash
        , m.project_contract_address
        , m.trade_category
        , 'secondary' AS trade_type
        , m.nft_contract_address
        , m.nft_token_id
        , m.nft_amount
        , m.buyer
        , m.seller
        , CASE
            WHEN m.currency_contract = 0x0000000000000000000000000000000000000000 THEN 0x000000000000000000000000000000000000800a -- Fix ETH
            WHEN m.currency_contract = 0x8Ebe4A94740515945ad826238Fc4D56c6B8b0e60 THEN 0x5aea5775959fbc2557cc8789bc1bf90a239d9a91 -- Fix WETH
            ELSE m.currency_contract
          END AS currency_contract
        , m.price_raw
        , CAST(COALESCE((pf.fee_percentage/100) * CAST(m.price_raw AS uint256),  DOUBLE '0') AS UINT256) AS platform_fee_amount_raw
        , COALESCE(roy.amount, uint256 '0') AS royalty_fee_amount_raw
        , CAST(null AS varbinary) AS platform_fee_address
        , roy.royaltyRecipient AS royalty_fee_address
        , m.evt_index AS sub_tx_trade_id
    FROM mintsquare_trades m
    LEFT JOIN royalties roy ON roy.block_time=m.block_time
        AND roy.tx_hash = m.tx_hash
        AND roy.nft_contract_address = m.nft_contract_address
        AND roy.nft_token_id = m.nft_token_id
        AND roy.id = m.id
    LEFT JOIN platform_fees pf ON pf.contract_address = m.strategy
)

-- this will be removed once tx_from and tx_to are available in the base event tables
{{ add_nft_tx_data('base_trades', 'zksync') }}
