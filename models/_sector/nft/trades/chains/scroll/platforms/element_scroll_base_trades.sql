{{ config(
    schema = 'element_scroll',
    alias = 'base_trades',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_number','tx_hash','sub_tx_trade_id'],
    )
}}

WITH element_txs AS (
        -- scroll ERC721 Sells
        SELECT 'scroll' AS blockchain
        , 'element' AS project
        , 'v1' AS project_version
        , ee.evt_block_time AS block_time
        , ee.erc721TokenId AS nft_token_id
        , 'secondary' AS trade_type
        , uint256 '1' AS nft_amount
        , 'Sell' AS trade_category
        , ee.maker AS seller
        , ee.taker AS buyer
        , ee.erc20TokenAmount AS price_raw
        , CASE WHEN ee.erc20Token=0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee THEN 0x0d500B1d8E8eF31E21C99d1Db9A6444d3ADf1270
            ELSE ee.erc20Token END AS currency_contract
        , ee.erc721Token AS nft_contract_address
        , ee.contract_address AS project_contract_address
        , ee.evt_tx_hash AS tx_hash
        , ee.evt_block_number AS block_number
        , ee.evt_index
        FROM {{ source('element_ex_scroll','ERC721OrdersFeature_evt_ERC721SellOrderFilled') }} ee
        {% if is_incremental() %}
        WHERE {{incremental_predicate('ee.evt_block_time')}}
        {% endif %}

        UNION ALL

        -- scroll ERC721 Buys
        SELECT 'scroll' AS blockchain
        , 'element' AS project
        , 'v1' AS project_version
        , ee.evt_block_time AS block_time
        , ee.erc721TokenId AS nft_token_id
        , 'secondary' AS trade_type
        , uint256 '1' AS nft_amount
        , 'Buy' AS trade_category
        , ee.taker AS seller
        , ee.maker AS buyer
        , ee.erc20TokenAmount AS price_raw
        , CASE WHEN ee.erc20Token=0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee THEN 0x0d500B1d8E8eF31E21C99d1Db9A6444d3ADf1270
            ELSE ee.erc20Token END AS currency_contract
        , ee.erc721Token AS nft_contract_address
        , ee.contract_address AS project_contract_address
        , ee.evt_tx_hash AS tx_hash
        , ee.evt_block_number AS block_number
        , ee.evt_index
        FROM {{ source('element_ex_scroll','ERC721OrdersFeature_evt_ERC721BuyOrderFilled') }} ee
        {% if is_incremental() %}
        WHERE {{incremental_predicate('ee.evt_block_time')}}
        {% endif %}
        )

, base_trades as (
    SELECT blockchain
    , project
    , project_version
    , block_time
    , cast(date_trunc('day', block_time) as date) as block_date
    , cast(date_trunc('month', block_time) as date) as block_month
    , nft_token_id
    , trade_type
    , nft_amount
    , trade_category
    , seller
    , buyer
    , price_raw
    , currency_contract
    , nft_contract_address
    , project_contract_address
    , tx_hash
    , block_number
    , uint256 '0' AS platform_fee_amount_raw
    , uint256 '0' AS royalty_fee_amount_raw
    , CAST(null AS varbinary) AS platform_fee_address
    , CAST(null AS varbinary) AS royalty_fee_address
    , evt_index as sub_tx_trade_id
    FROM element_txs
)

-- this will be removed once tx_from and tx_to are available in the base event tables
{{ add_nft_tx_data('base_trades', 'scroll') }}
