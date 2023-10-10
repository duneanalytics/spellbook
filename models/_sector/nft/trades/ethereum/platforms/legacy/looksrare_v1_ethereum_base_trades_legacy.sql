{{ config(
	tags=['legacy'],
	
    schema = 'looksrare_v1_ethereum',
    alias = alias('base_trades', legacy_model=True),
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_number','tx_hash','sub_tx_trade_id'],
    )
}}

WITH looksrare_trades AS (
    SELECT *
    , ROW_NUMBER() OVER (PARTITION BY tx_hash, nft_contract_address, nft_token_id ORDER BY evt_index ASC) AS id
    FROM (
        SELECT ta.evt_block_time AS block_time
        , ta.tokenId AS nft_token_id
        , ta.amount AS nft_amount
        , CASE WHEN ta.strategy='0x58d83536d3efedb9f7f2a1ec3bdaad2b1a4dd98c' THEN 'Private Sale' ELSE 'Buy' END AS trade_category
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
        FROM {{ source('looksrare_ethereum','LooksRareExchange_evt_TakerAsk') }} ta
        {% if is_incremental() %}
        WHERE ta.evt_block_time >= date_trunc("day", NOW() - interval '1 week')
        {% endif %}

        UNION ALL

        SELECT tb.evt_block_time AS block_time
        , tb.tokenId AS nft_token_id
        , tb.amount AS nft_amount
        , CASE WHEN tb.strategy='0x58d83536d3efedb9f7f2a1ec3bdaad2b1a4dd98c' THEN 'Private Sale' ELSE 'Offer Accepted' END AS trade_category
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
        FROM {{ source('looksrare_ethereum','LooksRareExchange_evt_TakerBid') }} tb
        {% if is_incremental() %}
        WHERE tb.evt_block_time >= date_trunc("day", NOW() - interval '1 week')
        {% endif %}
        )
    )

, royalties AS (
    SELECT evt_block_time AS block_time
    , evt_tx_hash AS tx_hash
    , evt_index
    , collection AS nft_contract_address
    , tokenId AS nft_token_id
    , amount
    , royaltyRecipient
    , ROW_NUMBER() OVER (PARTITION BY evt_tx_hash, collection, tokenId ORDER BY evt_index ASC) AS id
    FROM {{ source('looksrare_ethereum','LooksRareExchange_evt_RoyaltyPayment') }}
    {% if is_incremental() %}
    WHERE evt_block_time >= date_trunc("day", NOW() - interval '1 week')
    {% endif %}
    )

, platform_fees AS (
    SELECT distinct contract_address
    , output_0/100 AS fee_percentage
    FROM {{ source('looksrare_ethereum','StrategyStandardSaleForFixedPrice_call_viewProtocolFee') }}
    UNION ALL
    SELECT distinct contract_address
    , output_0/100 AS fee_percentage
    FROM {{ source('looksrare_ethereum','StrategyAnyItemFromCollectionForFixedPrice_call_viewProtocolFee') }}
    UNION ALL
    SELECT distinct contract_address
    , output_0/100 AS fee_percentage
    FROM {{ source('looksrare_ethereum','StrategyPrivateSale_call_viewProtocolFee') }}
    UNION ALL
    SELECT distinct contract_address
    , output_0/100 AS fee_percentage
    FROM {{ source('looksrare_ethereum','StrategyStandardSaleForFixedPriceV1B_call_viewProtocolFee') }}
    UNION ALL
    SELECT distinct contract_address
    , output_0/100 AS fee_percentage
    FROM {{ source('looksrare_ethereum','StrategyAnyItemFromCollectionForFixedPriceV1B_call_viewProtocolFee') }}
    )


SELECT
  date_trunc('day', lr.block_time) AS block_date
, lr.block_time
, lr.block_number
, lr.tx_hash
, lr.project_contract_address
, lr.trade_category
, 'secondary' as trade_type
, lr.nft_contract_address
, lr.nft_token_id
, lr.nft_amount
, lr.buyer
, lr.seller
, lr.currency_contract
, CAST(lr.price_raw AS DECIMAL(38,0)) AS price_raw
, CAST(COALESCE((pf.fee_percentage/100)*lr.price_raw, 0) as DECIMAL(38,0)) AS platform_fee_amount_raw
, CAST(COALESCE(roy.amount, 0) AS DECIMAL(38,0)) AS royalty_fee_amount_raw
, cast(null as varchar(1)) as platform_fee_address
, roy.royaltyRecipient AS royalty_fee_address
, lr.evt_index AS sub_tx_trade_id
FROM looksrare_trades lr
LEFT JOIN royalties roy ON roy.block_time=lr.block_time
    AND roy.tx_hash=lr.tx_hash
    AND roy.nft_contract_address=lr.nft_contract_address
    AND roy.nft_token_id=lr.nft_token_id
    AND roy.id = lr.id
LEFT JOIN platform_fees pf ON pf.contract_address=lr.strategy
