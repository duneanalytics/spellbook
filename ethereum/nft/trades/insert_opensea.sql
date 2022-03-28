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
        INNER JOIN nft.wyvern_data wc ON erc1155.evt_tx_hash = wc.call_tx_hash
        AND wc.token_id = erc1155.id::text
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
        INNER JOIN nft.wyvern_data wc ON erc721.evt_tx_hash = wc.call_tx_hash
        AND wc.token_id = erc721."tokenId"::text
        WHERE erc721."from" NOT IN ('\x0000000000000000000000000000000000000000')
        AND erc721.evt_block_time >= start_ts
        AND erc721.evt_block_time < end_ts
        GROUP BY evt_tx_hash,"tokenId"),

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
    wc.block_time,
    CASE WHEN agg.name is NOT NULL THEN tokens_agg.name
        ELSE tokens.name END AS nft_project_name,
    CASE WHEN erc20tr.evt_tx_hash = wc.call_tx_hash THEN null::text
        ELSE wc.token_id END AS nft_token_id, 
    CASE WHEN erc20tr.evt_tx_hash = wc.call_tx_hash THEN 'erc20'
        WHEN erc_values_1155.value_unique >= 1 THEN 'erc1155'
        WHEN erc_count_721.count_erc >= 1 THEN 'erc721'
        ELSE wc.erc_standard END AS erc_standard,
    'OpenSea' AS platform,
    '1' AS platform_version,
    CASE 
        WHEN agg.name is NOT NULL THEN 'Aggregator Trade'  -- Aggregator Trade, see aggregators.sql to see included addresses and aggregators 
        WHEN agg.name is NULL AND erc_values_1155.value_unique = 1 OR erc_count_721.count_erc = 1 THEN 'Single Item Trade'
        WHEN agg.name is NULL AND erc_values_1155.value_unique > 1 OR erc_count_721.count_erc > 1 THEN 'Bundle Trade'
    ELSE wc.trade_type END AS trade_type,
    -- Count number of items traded for different trade types and erc standards
    CASE 
        WHEN erc20tr.evt_tx_hash = wc.call_tx_hash THEN cast(wc.token_id as numeric)
        WHEN agg.name is NULL AND erc_values_1155.value_unique > 1 THEN cast(erc_values_1155.value_unique as numeric)
        WHEN agg.name is NULL AND erc_count_721.count_erc > 1 THEN cast(erc_count_721.count_erc as numeric)
        WHEN wc.trade_type = 'Single Item Trade' THEN cast(1 as numeric)
        WHEN wc.erc_standard = 'erc1155' THEN cast(erc_values_1155.value_unique as numeric)
        WHEN wc.erc_standard = 'erc721' THEN cast(erc_count_721.count_erc as numeric)
        ELSE (SELECT
                cast(count(1) as numeric) cnt
            FROM erc721."ERC721_evt_Transfer" erc721
            WHERE erc721.evt_tx_hash = wc.call_tx_hash
            AND erc721."from" NOT IN ('\x0000000000000000000000000000000000000000')
            AND erc721.evt_block_time >= start_ts and erc721.evt_block_time < end_ts
          ) +    
          (SELECT
               cast(count(1) as numeric)
            FROM erc1155."ERC1155_evt_TransferSingle" erc1155
            WHERE erc1155.evt_tx_hash = wc.call_tx_hash
            AND erc1155."from" NOT IN ('\x0000000000000000000000000000000000000000')
            AND erc1155.evt_block_time >= start_ts and erc1155.evt_block_time < end_ts
          ) END AS number_of_items,
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
    ROUND(cast((wc.fees/ 10 ^ erc20.decimals - 2.5*(wc.original_amount / 10 ^ erc20.decimals)/100) * (100) / NULLIF(wc.original_amount / 10 ^ erc20.decimals,0) as numeric),7) AS royalty_fees_percent,
    ROUND(cast(wc.fees/ 10 ^ erc20.decimals - 2.5*(wc.original_amount / 10 ^ erc20.decimals)/100 as numeric),7) AS original_royalty_fees,
    ROUND(cast(wc.fees/ 10 ^ erc20.decimals * p.price - 2.5 * (wc.original_amount / 10 ^ erc20.decimals * p.price) / 100 as numeric),7) AS usd_royalty_fees,
    -- Platform Fees (in %, amount in original currency and in USD)
    2.5 AS platform_fees_percent,
    ROUND(cast(2.5*(wc.original_amount / 10 ^ erc20.decimals)/100 as numeric),7) AS original_platform_fees,
    ROUND(cast(2.5*(wc.original_amount / 10 ^ erc20.decimals * p.price)/100 as numeric),7) AS usd_platform_fees, 
    CASE WHEN wc.original_currency_address[1] = '\x0000000000000000000000000000000000000000' THEN 'ETH' ELSE erc20.symbol END AS original_currency,
    wc.original_currency_address[1] AS original_currency_contract,
    wc.currency_token AS currency_contract,
    wc.nft_contract_address AS nft_contract_address,
    wc.exchange_contract_address, 
    wc.call_tx_hash AS tx_hash,
    wc.block_number,
    wc.tx_from,
    wc.tx_to,
    call_trace_address::integer[] as trace_address,
    NULL::integer AS evt_index,
    row_number() OVER (PARTITION BY wc.call_tx_hash ORDER BY wc.call_trace_address) AS trade_id
FROM nft.wyvern_data wc
LEFT JOIN erc_values_1155 ON erc_values_1155.evt_tx_hash = wc.call_tx_hash AND wc.token_id = erc_values_1155.token_id_erc
LEFT JOIN erc_count_721 ON erc_count_721.evt_tx_hash = wc.call_tx_hash AND wc.token_id = erc_count_721.token_id_erc
LEFT JOIN erc20."ERC20_evt_Transfer" erc20tr ON erc20tr.evt_tx_hash = wc.call_tx_hash AND wc.nft_contract_address = erc20tr.contract_address 
    AND erc20tr.evt_block_time >= start_ts
    AND erc20tr.evt_block_time < end_ts
LEFT JOIN nft.tokens tokens ON tokens.contract_address = wc.nft_contract_address
LEFT JOIN nft.tokens tokens_agg ON tokens_agg.contract_address = wc.nft_contract_address
LEFT JOIN nft.aggregators agg ON agg.contract_address = wc.tx_to
LEFT JOIN prices.usd p ON p.minute = date_trunc('minute', wc.block_time)
    AND p.contract_address = wc.currency_token
    AND p.minute >= start_ts
    AND p.minute < end_ts
LEFT JOIN prices.usd peth ON peth.minute = date_trunc('minute', wc.block_time)
    AND peth.symbol = 'WETH'
    AND peth.minute >= start_ts
    AND peth.minute < end_ts
LEFT JOIN erc20.tokens erc20 ON erc20.contract_address = wc.currency_token
WHERE
    wc.block_time >= start_ts
    AND wc.block_time < end_ts
ON CONFLICT DO NOTHING
    RETURNING 1
)
SELECT count(*) INTO r from rows;
RETURN r;
END
$function$;
