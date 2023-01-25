{{ config(
    schema = 'element_ethereum',
    alias = 'events',
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_time', 'unique_trade_id']
    )
}}

WITH element_txs AS (
        -- Ethereum ERC721 Sells
        SELECT 'ethereum' AS blockchain
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
        , CASE WHEN ee.erc20Token='0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' THEN '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
            ELSE ee.erc20Token END AS currency_contract
        , CASE WHEN ee.erc20Token='0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' THEN 'ETH' END AS currency_symbol
        , ee.erc721Token AS nft_contract_address
        , ee.contract_address AS project_contract_address
        , ee.evt_tx_hash AS tx_hash
        , ee.evt_block_number AS block_number
        FROM {{ source('element_ex_ethereum','OrdersFeature_evt_ERC721SellOrderFilled') }} ee
        {% if is_incremental() %}
        WHERE ee.evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}

        UNION ALL

        -- Ethereum ERC721 Buys
        SELECT 'ethereum' AS blockchain
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
        , CASE WHEN ee.erc20Token='0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' THEN '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
            ELSE ee.erc20Token END AS currency_contract
        , CASE WHEN ee.erc20Token='0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' THEN 'ETH' END AS currency_symbol
        , ee.erc721Token AS nft_contract_address
        , ee.contract_address AS project_contract_address
        , ee.evt_tx_hash AS tx_hash
        , ee.evt_block_number AS block_number
        FROM {{ source('element_ex_ethereum','OrdersFeature_evt_ERC721BuyOrderFilled') }} ee
        {% if is_incremental() %}
        WHERE ee.evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}

        UNION ALL

        -- Ethereum ERC1155 Sells
        SELECT 'ethereum' AS blockchain
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
        , CASE WHEN ee.erc20Token='0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' THEN '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
            ELSE ee.erc20Token END AS currency_contract
        , CASE WHEN ee.erc20Token='0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' THEN 'ETH' END AS currency_symbol
        , ee.erc1155Token AS nft_contract_address
        , ee.contract_address AS project_contract_address
        , ee.evt_tx_hash AS tx_hash
        , ee.evt_block_number AS block_number
        FROM {{ source('element_ex_ethereum','OrdersFeature_evt_ERC1155SellOrderFilled') }} ee
        {% if is_incremental() %}
        WHERE ee.evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}

        UNION ALL

        -- Ethereum ERC1155 Buys
        SELECT 'ethereum' AS blockchain
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
        , CASE WHEN ee.erc20Token='0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' THEN '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
            ELSE ee.erc20Token END AS currency_contract
        , CASE WHEN ee.erc20Token='0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' THEN 'ETH' END AS currency_symbol
        , ee.erc1155Token AS nft_contract_address
        , ee.contract_address AS project_contract_address
        , ee.evt_tx_hash AS tx_hash
        , ee.evt_block_number AS block_number
        FROM {{ source('element_ex_ethereum','OrdersFeature_evt_ERC1155BuyOrderFilled') }} ee
        {% if is_incremental() %}
        WHERE ee.evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
        )

SELECT alet.blockchain
, alet.project
, alet.version
, alet.block_time
, date_trunc('day', alet.block_time) AS block_date
, alet.token_id
, eth_nft_tokens.name AS collection
, alet.amount_raw/POWER(10, eth_erc20_tokens.decimals)*prices.price AS amount_usd
, alet.token_standard
, CASE WHEN agg.name IS NOT NULL THEN 'Bundle Trade' ELSE 'Single Item Trade' END AS trade_type
, CAST(alet.number_of_items AS DECIMAL(38,0)) AS number_of_items
, alet.trade_category
, 'Trade' AS evt_type
, alet.seller
, alet.buyer
, alet.amount_raw/POWER(10, eth_erc20_tokens.decimals) AS amount_original
, CAST(alet.amount_raw AS DECIMAL(38,0)) AS amount_raw
, COALESCE(alet.currency_symbol, eth_erc20_tokens.symbol) AS currency_symbol
, alet.currency_contract
, alet.nft_contract_address
, alet.project_contract_address
, agg.name AS aggregator_name
, CASE WHEN agg.name IS NOT NULL THEN agg.contract_address END AS aggregator_address
, alet.tx_hash
, alet.block_number
, et.from AS tx_from
, et.to AS tx_to
, CAST(0 AS DOUBLE) AS platform_fee_amount_raw
, CAST(0 AS DOUBLE) AS platform_fee_amount
, CAST(0 AS DOUBLE) AS platform_fee_amount_usd
, CAST(0 AS DOUBLE) AS platform_fee_percentage
, CAST(0 AS DOUBLE) AS royalty_fee_amount_raw
, CAST(0 AS DOUBLE) AS royalty_fee_amount
, CAST(0 AS DOUBLE) AS royalty_fee_amount_usd
, CAST(0 AS DOUBLE) AS royalty_fee_percentage
, CAST('0' AS VARCHAR(5)) AS royalty_fee_receive_address
, CAST('0' AS VARCHAR(5)) AS royalty_fee_currency_symbol
, alet.blockchain || alet.project || alet.version || alet.tx_hash || alet.seller  || alet.buyer || alet.nft_contract_address || alet.token_id AS unique_trade_id
FROM element_txs alet
LEFT JOIN {{ ref('nft_aggregators') }} agg ON alet.buyer=agg.contract_address AND agg.blockchain='ethereum'
LEFT JOIN {{ ref('tokens_nft') }} eth_nft_tokens ON eth_nft_tokens.contract_address=alet.nft_contract_address AND eth_nft_tokens.blockchain='ethereum'
LEFT JOIN {{ ref('tokens_erc20') }} eth_erc20_tokens ON eth_erc20_tokens.contract_address=alet.currency_contract AND eth_erc20_tokens.blockchain='ethereum'
LEFT JOIN {{ source('prices', 'usd') }} prices ON prices.minute=date_trunc('minute', alet.block_time)
    AND (prices.contract_address=alet.currency_contract AND prices.blockchain=alet.blockchain)
        {% if is_incremental() %}
        AND prices.minute >= date_trunc("day", now() - interval '1 week')
        {% endif %}
LEFT JOIN {{ source('ethereum','transactions') }} et ON et.hash=alet.tx_hash
    AND et.block_time=alet.block_time
        {% if is_incremental() %}
        AND et.block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
