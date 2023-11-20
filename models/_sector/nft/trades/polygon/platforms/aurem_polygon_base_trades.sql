{{ config(
    schema = 'aurem_polygon',
    
    alias = 'base_trades',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_number','tx_hash','sub_tx_trade_id'],
    )
}}

{% set project_start_date = "cast('2023-10-14' as timestamp)" %}

with trade_detail as (
    SELECT
          o.evt_block_time AS block_time
        , o.evt_block_number AS block_number
        , t.contract_address AS nft_contract_address
        , o.tokenId AS nft_token_id
        , uint256 '1' AS nft_amount
        , o.maker AS seller
        , o.taker AS buyer
        , CASE WHEN o.orderType = CAST(0 AS uint256) THEN 'Buy'
            WHEN o.orderType = CAST(1 AS uint256) THEN 'Sell'
            WHEN o.orderType = CAST(2 AS uint256) THEN 'BatchSell'
            ELSE 'Sell'
            END AS trade_category
        , 'secondary' AS trade_type
        , o.price AS price_raw
        , 0x0000000000000000000000000000000000000000 AS currency_contract
        , o.contract_address AS project_contract_address
        , o.evt_tx_hash AS tx_hash
        , uint256 '0' AS platform_fee_amount_raw
        , uint256 '0' AS royalty_fee_amount_raw
        , cast(NULL as varbinary) AS royalty_fee_address
        , cast(NULL as varbinary) as platform_fee_address
        , o.evt_index as sub_tx_trade_id
    FROM {{ source('aurem_polygon','Exchange_evt_OrderFilled') }} o
    INNER JOIN {{ source('erc721_polygon', 'evt_transfer') }} t ON o.evt_tx_hash = t.evt_tx_hash
        AND o.maker = t."from"
        AND o.taker = t."to"
        AND o.tokenId = t.tokenId
    {% if is_incremental() %}
    WHERE o.evt_block_time >= date_trunc('day', now() - interval '7' day)
        AND t.evt_block_time >= date_trunc('day', now() - interval '7' day)
    {% else %}
    WHERE o.evt_block_time >= {{project_start_date}}
        AND t.evt_block_time >= {{project_start_date}}
    {% endif %}
),

-- Payment token info is not passed in correctly, check from transfer to "Exchange" contract to find it out
payment_detail as (
    SELECT t.contract_address as currency_contract
        , d.tx_hash
        , d.sub_tx_trade_id
    FROM {{ source('erc20_polygon', 'evt_transfer') }} t
    INNER JOIN trade_detail d ON t.evt_tx_hash = d.tx_hash
        AND t."from" = d.buyer
        AND t."to" = 0x547eb9ab69f2e4438845839fd08792c326995ea6 -- Aurem Exchange
        AND t.value = d.price_raw
    {% if is_incremental() %}
        AND t.evt_block_time >= date_trunc('day', now() - interval '7' day)
    {% else %}
        AND t.evt_block_time >= {{project_start_date}}
    {% endif %}
)

SELECT
      d.block_time
    , d.block_number
    , d.nft_contract_address
    , d.nft_token_id
    , d.nft_amount
    , d.seller
    , d.buyer
    , d.trade_category
    , d.trade_type
    , d.price_raw
    , coalesce(p.currency_contract, d.currency_contract) AS currency_contract
    , d.project_contract_address
    , d.tx_hash
    , d.platform_fee_amount_raw
    , d.royalty_fee_amount_raw
    , d.royalty_fee_address
    , d.platform_fee_address
    , d.sub_tx_trade_id
FROM trade_detail d
LEFT JOIN payment_detail p ON d.tx_hash = p.tx_hash
    AND d.sub_tx_trade_id = p.sub_tx_trade_id
