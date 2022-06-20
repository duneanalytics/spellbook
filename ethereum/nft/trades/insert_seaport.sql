CREATE OR REPLACE FUNCTION nft.insert_seaport(start_ts timestamptz, end_ts timestamptz=now(), start_block numeric=0, end_block numeric=9e18) RETURNS INTEGER
LANGUAGE plpgsql AS $function$
DECLARE r INTEGER;
BEGIN

WITH seaport AS (
    SELECT block_number
    , block_time
    , buyer
    , 'Buy' AS category
    , currency_contract
    , erc_standard
    , erc_standard AS erc_types_array
    , NULL AS erc_values_array
    , evt_index
    , 'Trade' AS evt_type
    , exchange_contract_address
    , nft_contract_address
    , nft_contract_address AS nft_contract_address_array
    , nft_project_name
    , nft_token_id
    , nft_token_id AS nft_token_ids_array
    , number_of_items
    , original_amount
    , original_amount_raw
    , original_currency
    , original_currency_contract
    , platform
    , platform_version
    , buyer AS recipients_array
    , seller
    , seller AS senders_array
    , NULL AS trace_address
    , trade_type
    , tx_from
    , tx_hash
    , tx_to
    , usd_amount
    FROM seaport.view_transactions
    )

, ROWS AS (
    INSERT INTO nft.trades (
	block_time
	, nft_project_name
	, nft_token_id
    , erc_standard
	, platform
	, platform_version
    , trade_type
    , number_of_items
	, category
	, evt_type
	, usd_amount
	, seller
	, buyer
	, original_amount
	, original_amount_raw
	, original_currency
	, original_currency_contract
	, currency_contract
	, nft_contract_address
	, exchange_contract_address
	, tx_hash
	, block_number
    , nft_token_ids_array
    , senders_array
    , recipients_array
    , erc_types_array
    , nft_contract_addresses_array
    , erc_values_array
	, tx_from
	, tx_to
	, trace_address
	, evt_index
	, trade_id
    )

    SELECT block_time
    , nft_project_name
    , nft_token_id
    , erc_standard
    , platform
    , platform_version
    , CASE WHEN number_of_items > 1 THEN 'Bundle Trade' ELSE 'Single Item Trade' END AS trade_type
    , number_of_items
    , trades.category
    , trades.evt_type
    , usd_amount
    , trades.seller
    , trades.buyer
    , original_amount
    , original_amount_raw
    , original_currency
    , original_currency_contract
    , currency_contract
    , nft_contract_address
    , exchange_contract_address
    , tx_hash
    , block_number
    , nft_token_ids_array
    , senders_array
    , recipients_array
    , erc_types_array
    , nft_contract_addresses_array
    , erc_values_array
    , tx_from
    , tx_to
    , trace_address
    , evt_index
    , ROW_NUMBER() OVER (PARTITION BY platform, tx_hash, evt_index, category ORDER BY platform_version, evt_type) AS trade_id
    FROM seaport
    WHERE block_time >= start_ts
    AND block_time < end_ts
    ON CONFLICT DO NOTHING
    RETURNING 1
)
SELECT COUNT(*) INTO r FROM ROWS;
RETURN r;
END
$function$;

-- fill 2022
SELECT nft.insert_seaport(
    '2022-06-10'
    , NOW()
    , (SELECT MAX(number) FROM ethereum.blocks WHERE time < '2022-06-10')
    , (SELECT MAX(number) FROM ethereum.blocks WHERE time < NOW() - interval '20 minutes')
)
WHERE NOT EXISTS (
    SELECT *
    FROM nft.trades
    WHERE block_time > '2022-06-10'
    AND block_time <= NOW() - interval '20 minutes'
    AND platform = 'OpenSea'
    AND platform_version = 3
);


INSERT INTO cron.job (schedule, command)
VALUES ('/15 * * * *', $$
    SELECT nft.insert_seaport(
        (SELECT MAX(block_time) - interval '6 hours' FROM nft.trades WHERE platform='OpenSea' AND platform_version = 3)
        , (SELECT NOW() - interval '20 minutes')
        , (SELECT MAX(number) FROM ethereum.blocks WHERE time < (SELECT max(block_time) - interval '6 hours' FROM nft.trades WHERE platform='OpenSea' AND platform_version = 3))
        , (SELECT MAX(number) FROM ethereum.blocks WHERE time < NOW() - interval '20 minutes'));
$$)
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;