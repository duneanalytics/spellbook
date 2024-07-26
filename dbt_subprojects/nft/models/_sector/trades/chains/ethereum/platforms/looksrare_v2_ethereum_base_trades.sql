{{ config(
    schema = 'looksrare_v2_ethereum',
    
    alias = 'base_trades',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_number','tx_hash','sub_tx_trade_id'],
    )
}}

{% set looksrare_v2_start_date = "TIMESTAMP '2023-04-01'" %}

WITH looksrare_v2_trades AS (
    SELECT l.evt_block_time AS block_time
    , l.evt_block_number AS block_number
    , 'Offer Accepted' AS trade_category
    , l.feeAmounts[1]+l.feeAmounts[2]+l.feeAmounts[3] AS price_raw
    , l.askUser AS seller
    , l.bidUser AS buyer
    , l.collection AS nft_contract_address
    , element_at(l.amounts, 2) AS nft_amount
    , l.currency
    , l.itemIds[1] AS nft_token_id
    , l.contract_address AS project_contract_address
    , l.evt_tx_hash AS tx_hash
    , l.evt_index
    , l.feeAmounts[2] AS royalty_fee_amount_raw
    , l.feeAmounts[3] AS platform_fee_amount_raw
    , CASE WHEN l.feeRecipients[2]!=0x0000000000000000000000000000000000000000 THEN l.feeRecipients[2] END AS royalty_fee_address
--    , JSON_EXTRACT_SCALAR(l.nonceInvalidationParameters, '$.orderHash') AS order_hash
    FROM {{ source('looksrare_v2_ethereum','LooksRareProtocol_evt_TakerAsk') }} l
    {% if is_incremental() %}
    WHERE l.{{incremental_predicate('evt_block_time')}}
    {% else %}
    WHERE l.evt_block_time >= {{looksrare_v2_start_date}}
    {% endif %}

    UNION ALL

    SELECT l.evt_block_time AS block_time
    , l.evt_block_number AS block_number
    , 'Buy' AS trade_category
    , l.feeAmounts[1]+l.feeAmounts[2]+l.feeAmounts[3] AS price_raw
    , l.feeRecipients[1] AS seller
    , l.bidUser AS buyer
    , l.collection AS nft_contract_address
    , element_at(l.amounts, 2) AS nft_amount
    , l.currency
    , l.itemIds[1] AS nft_token_id
    , l.contract_address AS project_contract_address
    , l.evt_tx_hash AS tx_hash
    , l.evt_index
    , l.feeAmounts[2] AS royalty_fee_amount_raw
    , l.feeAmounts[3] AS platform_fee_amount_raw
    , CASE WHEN l.feeRecipients[2]!=0x0000000000000000000000000000000000000000 THEN l.feeRecipients[2] END AS  royalty_fee_address
--    , JSON_EXTRACT_SCALAR(l.nonceInvalidationParameters, '$.orderHash') AS order_hash
    FROM {{ source('looksrare_v2_ethereum','LooksRareProtocol_evt_TakerBid') }} l
    {% if is_incremental() %}
    WHERE l.{{incremental_predicate('evt_block_time')}}
    {% else %}
    WHERE l.evt_block_time >= {{looksrare_v2_start_date}}
    {% endif %}
    )

SELECT
 'ethereum' as blockchain
, 'looksrare' as project
, 'v2' as project_version
, block_time
, block_number
, tx_hash
, project_contract_address
, nft_contract_address
, nft_token_id
, nft_amount
, trade_category
, 'secondary' AS trade_type
, buyer
, seller
, currency AS currency_contract
, price_raw
, platform_fee_amount_raw
, royalty_fee_amount_raw
, royalty_fee_address
, CAST(null as varbinary) as platform_fee_address
, evt_index as sub_tx_trade_id
FROM looksrare_v2_trades
