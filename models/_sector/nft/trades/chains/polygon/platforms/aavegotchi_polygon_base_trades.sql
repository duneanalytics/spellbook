{{ config(
    schema = 'aavegotchi_polygon',
    alias = 'base_trades',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_number','tx_hash','sub_tx_trade_id'],
    )
}}

{% set nft_start_date = "CAST('2021-03-02' as timestamp)" %}

WITH trades AS (
    SELECT 'Buy' AS trade_category,
        evt_block_time,
        evt_block_number,
        evt_tx_hash,
        contract_address,
        evt_index as sub_tx_trade_id,
        'Trade' AS evt_type,
        buyer,
        seller,
        erc721TokenAddress AS nft_contract_address,
        erc721TokenId AS nft_token_id,
        uint256 '1' AS nft_amount,
        0x385eeac5cb85a38a9a07a70c73e0a3271cfb54a7 AS currency_contract, -- All sale are in GHST
        cast(priceInWei as uint256) AS price_raw
    FROM {{ source ('aavegotchi_polygon', 'aavegotchi_diamond_evt_ERC721ExecutedListing') }}
    WHERE 1 = 1
        {% if not is_incremental() %}
        AND evt_block_time >= {{nft_start_date}}
        {% endif %}
        {% if is_incremental() %}
        AND {{incremental_predicate('evt_block_time')}}
        {% endif %}

    UNION ALL

    SELECT 'Buy' AS trade_category,
        evt_block_time,
        evt_block_number,
        evt_tx_hash,
        contract_address,
        evt_index,
        'Trade' AS evt_type,
        buyer,
        seller,
        erc1155TokenAddress AS nft_contract_address,
        erc1155TypeId AS nft_token_id,
        cast(_quantity as bigint) AS nft_amount,
        0x385eeac5cb85a38a9a07a70c73e0a3271cfb54a7 AS currency_contract, -- All sale are in GHST
        priceInWei AS price_raw
    FROM {{ source ('aavegotchi_polygon', 'aavegotchi_diamond_evt_ERC1155ExecutedListing') }}
    WHERE 1 = 1
        {% if not is_incremental() %}
        AND evt_block_time >= {{nft_start_date}}
        {% endif %}
        {% if is_incremental() %}
        AND {{incremental_predicate('evt_block_time')}}
        {% endif %}
)
, base_trades as (
SELECT
    'polygon' AS blockchain,
    'aavegotchi' AS project,
    'v1' AS project_version,
    a.evt_tx_hash AS tx_hash,
    a.evt_block_time AS block_time,
    cast(date_trunc('day', evt_block_time) as date) as block_date,
    cast(date_trunc('month', evt_block_time) as date) as block_month,
    a.evt_block_number AS block_number,
    CAST(price_raw as uint256) AS price_raw,
    a.currency_contract,
    nft_token_id,
    'secondary' as trade_type
    a.contract_address AS project_contract_address,
    CASE WHEN number_of_items = 1 THEN 'Single Item Trade' ELSE 'Bundle Trade' END AS trade_type,
    CAST(number_of_items AS uint256) AS number_of_items,
    a.trade_category,
    a.buyer,
    a.seller,
    a.nft_contract_address,
    CAST(2 * price_raw / 100 AS uint256) AS platform_fee_price_raw,
    uint256 '0' AS royalty_fee_price_raw,
    CAST(NULL AS varbinary) AS royalty_fee_address,
    CAST(NULL AS varbinary) AS platform_fee_address,
FROM trades
)

-- this will be removed once tx_from and tx_to are available in the base event tables
{{ add_nft_tx_data('base_trades', 'polygon') }}
