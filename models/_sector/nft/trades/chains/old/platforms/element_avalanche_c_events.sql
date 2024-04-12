{{ config(
    schema = 'element_avalanche_c',
    alias = 'events',
    
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_time', 'unique_trade_id']
    )
}}

WITH element_txs AS (
        -- Avalanche ERC721 Sells
        SELECT 'avalanche_c' AS blockchain
        , 'element' AS project
        , 'v1' AS version
        , ee.evt_block_time AS block_time
        , ee.erc721TokenId AS token_id
        , 'erc721' AS token_standard
        , 'Single Item Trade' AS trade_type
        , 1 AS number_of_items
        , 'Offer Accepted' AS trade_category
        , ee.maker AS seller
        , ee.taker AS buyer
        , ee.erc20TokenAmount AS amount_raw
        , CASE WHEN ee.erc20Token=0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee THEN 0xb31f66aa3c1e785363f0875a1b74e27b85fd66c7
            ELSE ee.erc20Token END AS currency_contract
        , CASE WHEN ee.erc20Token=0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee THEN 'AVAX' END AS currency_symbol
        , ee.erc721Token AS nft_contract_address
        , ee.contract_address AS project_contract_address
        , ee.evt_tx_hash AS tx_hash
        , ee.evt_block_number AS block_number
        FROM {{ source('element_ex_avalanche_c','OrdersFeature_evt_ERC721SellOrderFilled') }} ee
        {% if is_incremental() %}
        WHERE ee.evt_block_time >= date_trunc('day', now() - interval '7' day)
        {% endif %}

        UNION ALL

        -- Avalanche ERC721 Buys
        SELECT 'avalanche_c' AS blockchain
        , 'element' AS project
        , 'v1' AS version
        , ee.evt_block_time AS block_time
        , ee.erc721TokenId AS token_id
        , 'erc721' AS token_standard
        , 'Single Item Trade' AS trade_type
        , 1 AS number_of_items
        , 'Buy' AS trade_category
        , ee.taker AS seller
        , ee.maker AS buyer
        , ee.erc20TokenAmount AS amount_raw
        , CASE WHEN ee.erc20Token=0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee THEN 0xb31f66aa3c1e785363f0875a1b74e27b85fd66c7
            ELSE ee.erc20Token END AS currency_contract
        , CASE WHEN ee.erc20Token=0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee THEN 'AVAX' END AS currency_symbol
        , ee.erc721Token AS nft_contract_address
        , ee.contract_address AS project_contract_address
        , ee.evt_tx_hash AS tx_hash
        , ee.evt_block_number AS block_number
        FROM {{ source('element_ex_avalanche_c','OrdersFeature_evt_ERC721BuyOrderFilled') }} ee
        {% if is_incremental() %}
        WHERE ee.evt_block_time >= date_trunc('day', now() - interval '7' day)
        {% endif %}

        UNION ALL

        -- Avalanche ERC1155 Sells
        SELECT 'avalanche_c' AS blockchain
        , 'element' AS project
        , 'v1' AS version
        , ee.evt_block_time AS block_time
        , ee.erc1155TokenId AS token_id
        , 'erc1155' AS token_standard
        , 'Single Item Trade' AS trade_type
        , 1 AS number_of_items
        , 'Offer Accepted' AS trade_category
        , ee.maker AS seller
        , ee.taker AS buyer
        , ee.erc20FillAmount AS amount_raw
        , CASE WHEN ee.erc20Token=0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee THEN 0xb31f66aa3c1e785363f0875a1b74e27b85fd66c7
            ELSE ee.erc20Token END AS currency_contract
        , CASE WHEN ee.erc20Token=0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee THEN 'AVAX' END AS currency_symbol
        , ee.erc1155Token AS nft_contract_address
        , ee.contract_address AS project_contract_address
        , ee.evt_tx_hash AS tx_hash
        , ee.evt_block_number AS block_number
        FROM {{ source('element_ex_avalanche_c','OrdersFeature_evt_ERC1155SellOrderFilled') }} ee
        {% if is_incremental() %}
        WHERE ee.evt_block_time >= date_trunc('day', now() - interval '7' day)
        {% endif %}

        UNION ALL

        -- Avalanche ERC1155 Buys
        SELECT 'avalanche_c' AS blockchain
        , 'element' AS project
        , 'v1' AS version
        , ee.evt_block_time AS block_time
        , ee.erc1155TokenId AS token_id
        , 'erc1155' AS token_standard
        , 'Single Item Trade' AS trade_type
        , 1 AS number_of_items
        , 'Buy' AS trade_category
        , ee.taker AS seller
        , ee.maker AS buyer
        , ee.erc20FillAmount AS amount_raw
        , CASE WHEN ee.erc20Token=0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee THEN 0xb31f66aa3c1e785363f0875a1b74e27b85fd66c7
            ELSE ee.erc20Token END AS currency_contract
        , CASE WHEN ee.erc20Token=0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee THEN 'AVAX' END AS currency_symbol
        , ee.erc1155Token AS nft_contract_address
        , ee.contract_address AS project_contract_address
        , ee.evt_tx_hash AS tx_hash
        , ee.evt_block_number AS block_number
        FROM {{ source('element_ex_avalanche_c','OrdersFeature_evt_ERC1155BuyOrderFilled') }} ee
        {% if is_incremental() %}
        WHERE ee.evt_block_time >= date_trunc('day', now() - interval '7' day)
        {% endif %}
        )

SELECT alet.blockchain
, alet.project
, alet.version
, alet.block_time
, alet.token_id
, ava_nft_tokens.name AS collection
, alet.amount_raw/POWER(10, ava_erc20_tokens.decimals)*prices.price AS amount_usd
, alet.token_standard
, CASE WHEN agg.name IS NOT NULL THEN 'Bundle Trade' ELSE 'Single Item Trade' END AS trade_type
, cast(alet.number_of_items as uint256) as number_of_items
, alet.trade_category
, 'Trade' AS evt_type
, alet.seller
, alet.buyer
, alet.amount_raw/POWER(10, ava_erc20_tokens.decimals) AS amount_original
, alet.amount_raw
, COALESCE(alet.currency_symbol, ava_erc20_tokens.symbol) AS currency_symbol
, alet.currency_contract
, alet.nft_contract_address
, alet.project_contract_address
, agg.name AS aggregator_name
, CASE WHEN agg.name IS NOT NULL THEN agg.contract_address END AS aggregator_address
, alet.tx_hash
, alet.block_number
, at."from" AS tx_from
, at.to AS tx_to
, uint256 '0' AS platform_fee_amount_raw
, DOUBLE '0' AS platform_fee_amount
, DOUBLE '0' AS platform_fee_amount_usd
, DOUBLE '0' AS platform_fee_percentage
, uint256 '0' AS royalty_fee_amount_raw
, DOUBLE '0' AS royalty_fee_amount
, DOUBLE '0' AS royalty_fee_amount_usd
, DOUBLE '0' AS royalty_fee_percentage
, CAST('0' AS varbinary) AS royalty_fee_receive_address
, CAST('0' AS VARCHAR) AS royalty_fee_currency_symbol
, alet.blockchain || alet.project || alet.version || cast(alet.tx_hash as varchar) || cast(alet.token_id as varchar) AS unique_trade_id
FROM element_txs alet
LEFT JOIN {{ ref('nft_aggregators') }} agg ON alet.buyer=agg.contract_address AND agg.blockchain='avalanche_c'
LEFT JOIN {{ source('tokens', 'erc20') }} ava_erc20_tokens ON ava_erc20_tokens.contract_address=alet.currency_contract AND ava_erc20_tokens.blockchain='avalanche_c'
LEFT JOIN {{ ref('tokens_nft') }} ava_nft_tokens ON ava_nft_tokens.contract_address=alet.currency_contract AND ava_nft_tokens.blockchain='avalanche_c'
LEFT JOIN {{ source('prices', 'usd') }} prices ON prices.minute=date_trunc('minute', alet.block_time)
    AND prices.contract_address=alet.currency_contract AND prices.blockchain='avalanche_c'
        {% if is_incremental() %}
        AND prices.minute >= date_trunc('day', now() - interval '7' day)
        {% endif %}
LEFT JOIN {{ source('avalanche_c','transactions') }} at ON at.hash=alet.tx_hash
    AND at.block_time=alet.block_time
        {% if is_incremental() %}
        AND at.block_time >= date_trunc('day', now() - interval '7' day)
        {% endif %}
