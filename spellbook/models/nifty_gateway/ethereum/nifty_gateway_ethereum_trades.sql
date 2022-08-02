{{ config(
        alias ='trades',
        )
}}

with nifty_gateway_sale as (
select
        a.*,
        b.seller,
        b.buyer,
        b.call_success,
        b.call_tx_hash,
        b.call_block_time,
        b.call_block_number
    FROM {{ source('nifty_gateway_ethereum', 'NiftyExchangeExecutor_evt_NiftySale721') }} a
    INNER JOIN  {{ source('nifty_gateway_ethereum', 'NiftyExchangeExecutor_call_executeSaleEth721') }} b
    ON a.evt_block_time = b.call_block_time)

, nifty_gateway AS (
    SELECT
        'NiftyGateway' AS platform,
        '1' AS platform_version,
        'Trade' AS evt_type,
        call_block_time AS block_time,
        "tokenId" AS token_id,
        1 as number_of_items,
        seller AS seller,
        buyer as buyer,
        price as price,
        CASE -- REPLACE `ETH` WITH `WETH` for ERC20 lookup later
            WHEN "priceCurrency" = '0x0000000000000000000000000000000000000000' THEN '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
            ELSE "priceCurrency"
        END AS currency_token,
        "priceCurrency" AS original_currency_address,
        "tokenContract" as nft_contract_address,
        contract_address as contract_address,
        evt_tx_hash AS tx_hash,
        evt_block_number AS block_number,
        evt_index AS evt_index,
        'Buy' as category
    FROM nifty_gateway_sale)

-- Get ERC721 AND ERC1155 transfer data for every trade TRANSACTION
, erc_union AS (
SELECT
    erc721.evt_tx_hash,
    'erc721' AS erc_type,
    erc721.tokenId AS tokenId,
    erc721.`from`,
    erc721.`to`,
    erc721.contract_address,
    NULL::NUMERIC AS VALUE
FROM {{ source('erc721_ethereum', 'evt_transfer') }} erc721
INNER JOIN {{ source('nifty_gateway_ethereum' , 'NiftyExchangeExecutor_evt_NiftySale721') }} nifty_sale
on nifty_sale.evt_tx_hash = erc721.evt_tx_hash


UNION ALL

SELECT
    erc1155.evt_tx_hash,
    'erc1155' AS erc_type,
    erc1155.id AS tokenId,
    erc1155.`from`,
    erc1155.`to`,
    erc1155.contract_address,
    erc1155.value
FROM {{ source('erc1155_ethereum', 'evt_transfersingle') }} erc1155
INNER JOIN  {{ source('nifty_gateway_ethereum', 'NiftyExchangeExecutor_evt_NiftySale721') }} nifty_sale
on  erc1155.evt_tx_hash = nifty_sale.evt_tx_hash)

-- aggregate NFT transfers per TRANSACTION
, niftygateway_erc_subsets AS (
SELECT
    evt_tx_hash,
    array_agg("tokenId") AS token_id_array,
    cardinality(array_agg("tokenId")) AS no_of_transfers,
    array_agg("from") AS from_array,
    array_agg("to") AS to_array,
    array_agg(erc_type) AS erc_type_array,
    array_agg(contract_address) AS contract_address_array,
    array_agg(VALUE) AS erc1155_value_array
FROM erc_union
GROUP BY 1
)

SELECT

    'ethereum'  as blockchain,
    trades.platform as project,
    trades.platform_version version,
    trades.block_time block_time,
    CASE WHEN erc.no_of_transfers > 1 THEN NULL ELSE token_id END as token_id,
    tokens.name as collection,
    trades.price/(10 ^ erc20.decimals::int) * p.price as amount_usd,
    CASE WHEN erc.no_of_transfers > 1 THEN NULL ELSE COALESCE(erc.erc_type_array[1], tokens.standard) END token_standard,
    CASE WHEN erc.no_of_transfers > 1 THEN 'Bundle Trade' ELSE 'Single Item Trade' END as trade_type,
    erc.no_of_transfers AS number_of_items,
    trades.category as trade_category,
    trades.evt_type as evt_type,
    trades.seller as seller,
    trades.buyer as buyer,
    trades.price/(10 ^ erc20.decimals::int) as amount_original,
    trades.price as amount_raw,
    CASE WHEN trades.original_currency_address = '0x0000000000000000000000000000000000000000' THEN 'ETH' ELSE erc20.symbol END AS currency_symbol,
    trades.original_currency_address as currency_contract,
    COALESCE(erc.contract_address_array[1], trades.nft_contract_address) AS nft_contract_address,
    trades.contract_address  as project_contract_address,
    NULL::string as aggregator_name,
    NULL::string as aggregator_address,
    trades.block_number as block_number,
    trades.tx_hash as tx_hash,
    tx.`from` as tx_from,
    tx.`to` as tx_to,
    ROW_NUMBER() OVER (PARTITION BY trades.platform, trades.tx_hash, trades.evt_index, trades.category ORDER BY trades.platform_version, trades.evt_type)  as unique_trade_id
    FROM nifty_gateway as trades
    INNER JOIN {{ source('ethereum', 'transactions') }} tx ON trades.tx_hash = tx.hash
    LEFT JOIN niftygateway_erc_subsets erc ON erc.evt_tx_hash = trades.tx_hash
    LEFT JOIN {{ ref('tokens_ethereum_nft') }} as tokens ON tokens.contract_address = trades.nft_contract_address
    LEFT JOIN {{ source('prices', 'usd') }} p ON p.minute = date_trunc('minute', trades.block_time)
        AND p.contract_address = trades.currency_token
    LEFT JOIN {{ ref('tokens_ethereum_erc20') }} erc20 ON erc20.contract_address = trades.currency_token


