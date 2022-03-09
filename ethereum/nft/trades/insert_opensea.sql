CREATE OR REPLACE FUNCTION nft.insert_opensea(start_ts timestamptz, end_ts timestamptz=now(), start_block numeric=0, end_block numeric=9e18) RETURNS integer
LANGUAGE plpgsql AS $function$
DECLARE r integer;
BEGIN

-- Get ERC1155 token ID and number of token IDs for every trade transaction 
WITH erc_values_1155 as
(SELECT evt_tx_hash,
        id::text as token_id_erc,
        cardinality(array_agg(value)) as card_values,
        value as value_unique
        FROM erc1155."ERC1155_evt_TransferSingle" erc1155
        WHERE erc1155."from" NOT IN ('\x0000000000000000000000000000000000000000')
        AND erc1155.evt_block_time >= start_ts
        AND erc1155.evt_block_time < end_ts
        GROUP BY evt_tx_hash,value,id),

-- Get ERC721 token ID and number of token IDs for every trade transaction 
erc_count_721 as
(SELECT evt_tx_hash,
        "tokenId"::text as token_id_erc,
        COUNT("tokenId") as count_erc
        FROM erc721."ERC721_evt_Transfer" erc721
        WHERE erc721."from" NOT IN ('\x0000000000000000000000000000000000000000')
        AND erc721.evt_block_time >= start_ts
        AND erc721.evt_block_time < end_ts
        GROUP BY evt_tx_hash,"tokenId")

rows AS (
    INSERT INTO nft.trades(
    block_time,
    nft_project_name,
    nft_token_id,
    erc_standard,
    platform,
    platform_version,
    trade_type,
    number_of_items,
    category,
    evt_type,
    aggregator,
    usd_amount,
    seller,
    buyer,
    original_amount,
    original_amount_raw,
    eth_amount,
    royalty_fees_percent,
    original_royalty_fees,
    usd_royalty_fees,
    platform_fees_percent,
    original_platform_fees,
    usd_platform_fees,
    original_currency,
    original_currency_contract,
    currency_contract,
    nft_contract_address,
    exchange_contract_address,
    tx_hash,
    block_number,
    tx_from,
    tx_to,
    trace_address,
    evt_index,
    trade_id
    )

SELECT
    tx.block_time AS block_time,
    tokens.name AS nft_project_name,
    wc.token_id AS nft_token_id, 
    wc.erc_standard AS erc_standard,
    'OpenSea' AS platform,
    '1' AS platform_version,
    CASE 
    WHEN agg.name is NOT NULL THEN 'Aggregator Trade'  -- Aggregator Trade, see aggregators.sql to see included addresses and aggregators 
    WHEN agg.name is NULL AND erc_values_1155.value_unique = 1 OR erc_count_721.count_erc = 1 THEN 'Single Item Trade'
    WHEN agg.name is NULL AND erc_values_1155.value_unique > 1 OR erc_count_721.count_erc > 1 THEN 'Bundle Trade'
    ELSE wc.trade_type END AS trade_type,
    CASE -- Count number of items traded for different trade types and erc standards
    WHEN wc.erc_standard = 'erc1155' THEN erc_values_1155.value_unique
    WHEN wc.erc_standard = 'erc721' THEN erc_count_721.count_erc
    WHEN wc.trade_type = 'Single Item Trade' THEN 1
    ELSE (SELECT
                count(1) cnt
            FROM erc721."ERC721_evt_Transfer" erc721
            WHERE erc721.evt_tx_hash = wc.call_tx_hash
            AND erc721."from" NOT IN ('\x0000000000000000000000000000000000000000')
          ) +    
          (SELECT
                count(1) cnt
            FROM erc1155."ERC1155_evt_TransferSingle" erc1155
            WHERE erc1155.evt_tx_hash = wc.call_tx_hash
            AND erc1155."from" NOT IN ('\x0000000000000000000000000000000000000000')
          )
    END AS number_of_items, 
    'Buy' AS category,
    'Trade' AS evt_type,
    agg.name AS aggregator,
    wc.original_amount / 10 ^ erc20.decimals * p.price AS usd_amount,
    wc.seller AS seller,
    CASE WHEN agg.name is NULL THEN wc.buyer
         ELSE wc.buyer_when_aggr END AS buyer,
    wc.original_amount / 10 ^ erc20.decimals AS original_amount,
    wc.original_amount AS original_amount_raw,
    wc.original_amount / 10 ^ erc20.decimals * p.price / peth.price AS eth_amount,
    -- Royalty Fees (in %, amount in original currency and in USD)
    ROUND(cast((wc.fees - 2.5*(wc.original_amount / 10 ^ erc20.decimals)/100) * (100) / NULLIF(wc.original_amount / 10 ^ erc20.decimals,0) as numeric),7) AS royalty_fees_percent,
    ROUND(cast(wc.fees - 2.5*(wc.original_amount / 10 ^ erc20.decimals)/100 as numeric),7) AS original_royalty_fees,
    ROUND(cast(wc.fees * p.price - 2.5 * (wc.original_amount / 10 ^ erc20.decimals * p.price) / 100 as numeric),7) AS usd_royalty_fees,
    -- Platform Fees (in %, amount in original currency and in USD)
    2.5 AS platform_fees_percent,
    ROUND(cast(2.5*(wc.original_amount / 10 ^ erc20.decimals)/100 as numeric),7) AS original_platform_fees,
    ROUND(cast(2.5*(wc.original_amount / 10 ^ erc20.decimals * p.price)/100 as numeric),7) AS usd_platform_fees, 
    CASE WHEN wc.original_currency_address[1] = '\x0000000000000000000000000000000000000000' THEN 'ETH' ELSE erc20.symbol END AS original_currency,
    wc.original_currency_address[1] AS original_currency_contract,
    wc.currency_token AS currency_contract,
    CASE WHEN agg.name is NULL THEN wc.nft_contract_address
         ELSE wc.nft_contract_address_when_aggr END AS nft_contract_address,
    wc.exchange_contract_address, 
    wc.call_tx_hash AS tx_hash,
    tx.block_number,
    tx."from" AS tx_from,
    tx."to" AS tx_to,
    call_trace_address::integer[] as trace_address,
    NULL::integer AS evt_index,
    row_number() OVER (PARTITION BY wc.call_tx_hash ORDER BY wc.call_trace_address) AS trade_id
FROM ethereum.transactions tx
LEFT JOIN nft.wyvern_data wc ON wc.call_tx_hash = tx.hash
LEFT JOIN erc_values_1155 ON erc_values_1155.evt_tx_hash = tx.hash AND wc.token_id = erc_values_1155.token_id_erc
LEFT JOIN erc_count_721 ON erc_count_721.evt_tx_hash = tx.hash AND wc.token_id = erc_count_721.token_id_erc
LEFT JOIN nft.tokens tokens ON tokens.contract_address = wc.nft_contract_address
LEFT JOIN nft.aggregators agg ON agg.contract_address = tx."to"
LEFT JOIN prices.usd p ON p.minute = date_trunc('minute', tx.block_time)
    AND p.contract_address = wc.currency_token
LEFT JOIN prices.usd peth ON peth.minute = date_trunc('minute', tx.block_time)
    AND peth.contract_address = wc.currency_token
LEFT JOIN erc20.tokens erc20 ON erc20.contract_address = wc.currency_token
WHERE
        NOT EXISTS (SELECT * -- Exclude OpenSea mint transactions
        FROM erc721."ERC721_evt_Transfer" erc721
        WHERE wc.call_tx_hash = erc721.evt_tx_hash
        AND erc721."from" = '\x0000000000000000000000000000000000000000')
        AND peth.symbol = 'WETH'
        AND tx.block_time >= start_ts
        AND tx.block_time < end_ts
ON CONFLICT DO NOTHING
    RETURNING 1
)
SELECT count(*) INTO r from rows;
RETURN r;
END
$function$;