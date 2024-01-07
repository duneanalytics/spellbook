{{ config(
    schema = 'aurem_polygon',

    alias = 'base_trades',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_number','tx_hash','sub_tx_trade_id'],
    )
}}

{% set project_start_date = "cast('2023-10-01' as timestamp)" %}

with trade_detail as (
    SELECT
          o.evt_block_time AS block_time
        , o.evt_block_number AS block_number
        , t.contract_address AS nft_contract_address
        , o.tokenId AS nft_token_id
        , t.amount AS nft_amount
        , o.maker AS seller
        , o.taker AS buyer
        , CASE WHEN o.orderType = CAST(0 AS uint256) THEN 'Buy'
            WHEN o.orderType = CAST(1 AS uint256) THEN 'Sell'
            WHEN o.orderType = CAST(2 AS uint256) THEN 'BatchSell'
            ELSE 'Sell'
            END AS trade_category
        , 'secondary' AS trade_type
        , o.price AS price_raw
        , 0x0000000000000000000000000000000000001010 AS currency_contract
        , o.contract_address AS project_contract_address
        , o.evt_tx_hash AS tx_hash
        , uint256 '0' AS platform_fee_amount_raw
        , uint256 '0' AS royalty_fee_amount_raw
        , cast(NULL as varbinary) AS royalty_fee_address
        , cast(NULL as varbinary) as platform_fee_address
        , o.evt_index as sub_tx_trade_id
    FROM {{ source('aurem_polygon','Exchange_evt_OrderFilled') }} o
    INNER JOIN {{ ref('nft_polygon_transfers') }} t ON o.evt_tx_hash = t.tx_hash
        AND o.maker = t."from"
        AND o.taker = t."to"
        AND o.tokenId = t.token_id
    {% if is_incremental() %}
    WHERE {{incremental_predicate('o.evt_block_time')}}
       AND {{incremental_predicate('t.block_time')}}
    {% else %}
    WHERE o.evt_block_time >= {{project_start_date}}
        AND t.block_time >= {{project_start_date}}
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
        AND {{incremental_predicate('t.evt_block_time')}}
    {% else %}
        AND t.evt_block_time >= {{project_start_date}}
    {% endif %}
)
, base_trades as (
SELECT 'polygon' as blockchain
    , 'aurem' as project
    , 'v1' as project_version
    , d.block_time
    , cast(date_trunc('day', d.block_time) as date) as block_date
    , cast(date_trunc('month', d.block_time) as date) as block_month
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
)

-- this will be removed once tx_from and tx_to are available in the base event tables
{{ add_nft_tx_data('base_trades', 'polygon') }}
