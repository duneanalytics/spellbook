{{ config(
    schema = 'magiceden_ethereum',
    
    alias = 'base_trades',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_number','tx_hash','sub_tx_trade_id'],
    )
}}

{% set magiceden_start_date = "TIMESTAMP '2024-02-16'" %}

WITH trades AS (
    SELECT evt_block_time AS block_time
    , evt_block_number AS block_number
    , evt_tx_hash AS tx_hash
    , contract_address AS project_contract_address
    , buyer
    , seller
    , tokenAddress AS nft_contract_address
    , tokenId AS nft_token_id
    , 1 AS nft_amount
    , 'Buy' AS trade_category
    , paymentCoin AS currency_contract
    , salePrice AS price_raw
    , evt_index AS sub_tx_trade_id
    FROM {{ source('limitbreak_ethereum','PaymentProcessor_evt_BuyListingERC721') }}
    {% if is_incremental() %}
    WHERE {{incremental_predicate('evt_block_time')}}
    {% else %}
    WHERE evt_block_time >= {{magiceden_start_date}}
    {% endif %}
    
    UNION ALL
    
    SELECT evt_block_time AS block_time
    , evt_block_number AS block_number
    , evt_tx_hash AS tx_hash
    , contract_address AS project_contract_address
    , buyer
    , seller
    , tokenAddress AS nft_contract_address
    , tokenId AS nft_token_id
    , 1 AS nft_amount
    , 'Buy' AS trade_category
    , paymentCoin AS currency_contract
    , salePrice AS price_raw
    , evt_index AS sub_tx_trade_id
    FROM {{ source('limitbreak_ethereum','PaymentProcessor_evt_BuyListingERC1155') }}
    {% if is_incremental() %}
    WHERE {{incremental_predicate('evt_block_time')}}
    {% else %}
    WHERE evt_block_time >= {{magiceden_start_date}}
    {% endif %}
    
    UNION ALL
    
    SELECT evt_block_time AS block_time
    , evt_block_number AS block_number
    , evt_tx_hash AS tx_hash
    , contract_address AS project_contract_address
    , buyer
    , seller
    , tokenAddress AS nft_contract_address
    , tokenId AS nft_token_id
    , 1 AS nft_amount
    , 'Offer Accepted' AS trade_category
    , paymentCoin AS currency_contract
    , salePrice AS price_raw
    , evt_index AS sub_tx_trade_id
    FROM {{ source('limitbreak_ethereum','PaymentProcessor_evt_AcceptOfferERC721') }}
    {% if is_incremental() %}
    WHERE {{incremental_predicate('evt_block_time')}}
    {% else %}
    WHERE evt_block_time >= {{magiceden_start_date}}
    {% endif %}
    
    UNION ALL
    
    SELECT evt_block_time AS block_time
    , evt_block_number AS block_number
    , evt_tx_hash AS tx_hash
    , contract_address AS project_contract_address
    , buyer
    , seller
    , tokenAddress AS nft_contract_address
    , tokenId AS nft_token_id
    , amount AS nft_amount
    , 'Offer Accepted' AS trade_category
    , paymentCoin AS currency_contract
    , salePrice AS price_raw
    , evt_index AS sub_tx_trade_id
    FROM {{ source('limitbreak_ethereum','PaymentProcessor_evt_AcceptOfferERC1155') }}
    {% if is_incremental() %}
    WHERE {{incremental_predicate('evt_block_time')}}
    {% else %}
    WHERE evt_block_time >= {{magiceden_start_date}}
    {% endif %}
    )
    
, whitelisted_trades AS (
    SELECT t.block_time
    , t.block_number
    , t.tx_hash
    , t.project_contract_address
    , t.buyer
    , t.seller
    , t.nft_contract_address
    , t.nft_token_id
    , t.nft_amount
    , t.trade_category
    , t.currency_contract
    , t.price_raw
    , t.sub_tx_trade_id
    , fc.message
    FROM trades t
    INNER JOIN {{ source('limitbreak_ethereum','TrustedForwarder_call_forwardCall') }} fc ON fc.call_block_number=t.block_number
        AND fc.call_tx_hash=t.tx_hash
        AND fc.contract_address = 0x5ebc127fae83ed5bdd91fc6a5f5767e259df5642
        {% if is_incremental() %}
        AND {{incremental_predicate('call_block_time')}}
        {% else %}
        AND call_block_time >= {{magiceden_start_date}}
        {% endif %}
    )

, bundled_whitelisted_trades AS (
    SELECT block_number
    , tx_hash
    , array_agg(seller) AS sellers
    FROM whitelisted_trades
    GROUP BY 1, 2
    )

, fees AS (
    SELECT tr.block_number
    , tr.tx_hash
    , contract_address
    , 0xca9337244b5f04cb946391bc8b8a980e988f9a6a AS platform_fee_address
    , MAX_BY(tr.to, tr.amount_raw) FILTER (WHERE to != 0xca9337244b5f04cb946391bc8b8a980e988f9a6a) AS royalty_fee_address
    , SUM(tr.amount_raw) FILTER (WHERE to = 0xca9337244b5f04cb946391bc8b8a980e988f9a6a) AS platform_fee_amount_raw
    , SUM(tr.amount_raw) FILTER (WHERE to != 0xca9337244b5f04cb946391bc8b8a980e988f9a6a) AS royalty_fee_amount_raw
    FROM tokens_ethereum.transfers tr
    INNER JOIN bundled_whitelisted_trades wt ON tr.block_number=wt.block_number
        AND tr.tx_hash=wt.tx_hash
        AND tr.amount_raw > 0
        AND tr."from" = 0x9a1d00bed7cd04bcda516d721a596eb22aac6834
        AND NOT contains(wt.sellers, tr."to")
        AND tr.block_number >= 19242536
    {% if is_incremental() %}
    WHERE tr.{{incremental_predicate('block_time')}}
    {% else %}
    WHERE tr.block_time >= {{magiceden_start_date}}
    {% endif %}
    GROUP BY 1, 2, 3
    )

SELECT 'ethereum' as blockchain
, 'magiceden' as project
, 'v1' as project_version
, t.block_time
, CAST(date_trunc('day', t.block_time) AS date) AS block_date
, CAST(date_trunc('month', t.block_time) as date) as block_month
, t.block_number
, t.tx_hash
, t.project_contract_address
, t.buyer
, t.seller
, t.nft_contract_address
, t.nft_token_id
, t.nft_amount
, 'secondary' AS trade_type
, t.trade_category
, t.currency_contract
, t.price_raw
, f.platform_fee_amount_raw
, f.royalty_fee_amount_raw
, CASE WHEN platform_fee_amount_raw > 0 THEN f.platform_fee_address END AS platform_fee_address
, f.royalty_fee_address
, t.sub_tx_trade_id
FROM whitelisted_trades t
LEFT JOIN fees f ON t.block_number=f.block_number
    AND t.tx_hash=f.tx_hash
    AND (f.contract_address=t.currency_contract OR (f.contract_address=0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee AND t.currency_contract=0x0000000000000000000000000000000000000000))