{{ config(
        alias ='native_mints',
		materialized = 'incremental',
		file_format = 'delta',
		incremental_strategy = 'merge',
        unique_key='unique_trade_id',
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "sector",
                                    "nft",
                                    \'["umer_h_adil", "hildobby"]\') }}')
}}


WITH nft_mints AS (
    SELECT evt_block_time, evt_block_number, evt_tx_hash, contract_address, from, to, tokenId AS token_id, 'erc721' AS standard, evt_index
    , 1 AS amount
    FROM {{ source('erc721_ethereum','evt_transfer') }}
    WHERE from='0x0000000000000000000000000000000000000000'
    AND to NOT IN (SELECT address FROM addresses_ethereum.defi) -- We're interested in collectible NFTs (e.g. BAYC), not functional NFTs (e.g. Uniswap LP), so we exclude NFTs originated in DeFi
    {% if is_incremental() %}
    AND evt_block_time >= date_trunc("day", NOW() - interval '1 week')
    {% endif %}
    UNION
    SELECT evt_block_time, evt_block_number, evt_tx_hash, contract_address, from, to, id AS token_id, 'erc1155' AS standard, evt_index
    , value AS amount
    FROM {{ source('erc1155_ethereum','evt_transfersingle') }}
    WHERE from='0x0000000000000000000000000000000000000000'
    AND to NOT IN (SELECT address FROM addresses_ethereum.defi) -- We're interested in collectible NFTs (e.g. BAYC), not functional NFTs (e.g. Uniswap LP), so we exclude NFTs originated in DeFi
    {% if is_incremental() %}
    AND evt_block_time >= date_trunc("day", NOW() - interval '1 week')
    {% endif %}
    UNION
    SELECT evt_block_time, evt_block_number, evt_tx_hash, contract_address, from, to
    , ids_and_count.ids AS token_id
    , 'erc1155' AS standard, evt_index
    , ids_and_count.values AS amount
    FROM (
        SELECT evt_block_time, evt_block_number, evt_tx_hash, contract_address, from, to, evt_index
        , explode(arrays_zip(values, ids)) AS ids_and_count
        FROM {{ source('erc1155_ethereum','evt_transferbatch') }}
        WHERE from='0x0000000000000000000000000000000000000000'
        AND to NOT IN (SELECT address FROM addresses_ethereum.defi) -- We're interested in collectible NFTs (e.g. BAYC), not functional NFTs (e.g. Uniswap LP), so we exclude NFTs originated in DeFi
      {% if is_incremental() %}
      AND evt_block_time >= date_trunc("day", NOW() - interval '1 week')
      {% endif %}
        GROUP BY evt_block_time, evt_block_number, evt_tx_hash, contract_address, from, to, evt_index, values, ids
        )
    WHERE ids_and_count.values > 0
    GROUP BY evt_block_time, evt_block_number, evt_tx_hash, contract_address, from, to, evt_index, token_id, amount
    )

, namespaces AS (
    SELECT address
    , FIRST(namespace) AS namespace
	FROM {{ source('ethereum','contracts') }}
	GROUP BY address
	)

, nfts_per_tx AS (
    SELECT evt_tx_hash
    , COUNT(*) AS nfts_minted_in_tx
    FROM nft_mints
    GROUP BY evt_tx_hash
    )

SELECT 'ethereum' AS blockchain
, COALESCE(ec.namespace, 'Unknown') AS project
, NULL AS version
, nft_mints.evt_block_time AS block_time
, date_trunc('day', nft_mints.evt_block_time) AS block_date
, nft_mints.evt_block_number AS block_number
, nft_mints.token_id AS token_id
, tok.name AS collection
, nft_mints.standard AS token_standard
, CASE WHEN nft_mints.amount=1 THEN 'Single Item Mint'
    ELSE 'Bundle Mint'
    END AS trade_type
, nft_mints.amount AS number_of_items
, 'Mint' AS trade_category
, 'Mint' AS evt_type
, nft_mints.from AS seller
, nft_mints.to AS buyer
, COALESCE(SUM(et.value), SUM(erc20s.value), 0)*(nft_mints.amount/nft_count.nfts_minted_in_tx) AS amount_raw
, COALESCE(SUM(et.value)/POWER(10, 18), SUM(erc20s.value)/POWER(10, pu_erc20s.decimals))*(nft_mints.amount/nft_count.nfts_minted_in_tx) AS amount_original
, COALESCE(pu_eth.price*SUM(et.value)/POWER(10, 18), pu_erc20s.price*SUM(erc20s.value)/POWER(10, pu_erc20s.decimals))*(nft_mints.amount/nft_count.nfts_minted_in_tx) AS amount_usd
, CASE WHEN et.success THEN 'ETH' ELSE pu_erc20s.symbol END AS currency_symbol
, CASE WHEN et.success THEN '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' ELSE erc20s.contract_address END AS currency_contract
, nft_mints.contract_address AS nft_contract_address
, etxs.to AS project_contract_address
, agg.name AS aggregator_name
, agg.contract_address AS aggregator_address
, nft_mints.evt_tx_hash AS tx_hash
, etxs.from AS tx_from
, etxs.to AS tx_to
, 0 AS platform_fee_amount_raw
, 0 AS platform_fee_amount
, 0 AS platform_fee_amount_usd
, 0 AS platform_fee_percentage
, NULL AS royalty_fee_receive_address
, 0 AS royalty_fee_currency_symbol
, 0 AS royalty_fee_amount_raw
, 0 AS royalty_fee_amount
, 0 AS royalty_fee_amount_usd
, 0 AS royalty_fee_percentage
, 'ethereum' || '-' || COALESCE(ec.namespace, 'Unknown') || '-Mint-' || COALESCE(nft_mints.evt_tx_hash, '-1') || '-' || COALESCE(nft_mints.to, '-1') || '-' ||  COALESCE(nft_mints.contract_address, '-1') || '-' || COALESCE(nft_mints.token_id, '-1') || COALESCE(nft_mints.evt_index, '-1') AS unique_trade_id
FROM nft_mints nft_mints
LEFT JOIN nfts_per_tx nft_count ON nft_count.evt_tx_hash=nft_mints.evt_tx_hash
LEFT JOIN {{ source('ethereum','traces') }} et ON et.block_time=nft_mints.evt_block_time
    AND et.tx_hash=nft_mints.evt_tx_hash
    AND et.from=nft_mints.to
    AND (et.call_type NOT IN ('delegatecall', 'callcode', 'staticcall') OR et.call_type IS NULL)
    AND et.success
    AND et.value > 0
LEFT JOIN {{ source('prices','usd') }} pu_eth ON pu_eth.blockchain='ethereum'
    AND pu_eth.minute=date_trunc('minute', et.block_time)
    AND pu_eth.contract_address='0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
LEFT JOIN erc20_ethereum.evt_Transfer erc20s ON erc20s.evt_block_time=nft_mints.evt_block_time
    AND erc20s.from=nft_mints.to
LEFT JOIN {{ source('prices','usd') }} pu_erc20s ON pu_erc20s.blockchain='ethereum'
    AND pu_erc20s.minute=date_trunc('minute', erc20s.evt_block_time)
    AND erc20s.contract_address=pu_erc20s.contract_address
LEFT JOIN {{ source('ethereum','transactions') }} etxs ON etxs.block_time=nft_mints.evt_block_time
    AND etxs.hash=nft_mints.evt_tx_hash
LEFT JOIN {{ ref('nft_ethereum_aggregators') }} agg ON etxs.to=agg.contract_address
LEFT JOIN {{ ref('tokens_nft') }} tok ON tok.contract_address=nft_mints.contract_address
LEFT JOIN namespaces ec ON etxs.to=ec.address
{% if is_incremental() %}
WHERE nft_mints.evt_block_time >= date_trunc("day", now() - interval '1 week')
AND  et.block_time >= date_trunc("day", now() - interval '1 week')
AND  pu_eth.minute >= date_trunc("day", now() - interval '1 week')
AND  pu_erc20s.minute >= date_trunc("day", now() - interval '1 week')
AND  etxs.block_time >= date_trunc("day", now() - interval '1 week')
{% endif %}
GROUP BY nft_mints.evt_block_time, nft_mints.evt_block_number, nft_mints.token_id, nft_mints.standard
, nft_mints.amount, nft_mints.from, nft_mints.to, nft_mints.contract_address, etxs.to
, nft_mints.evt_tx_hash, etxs.from, ec.namespace, tok.name, pu_erc20s.decimals, pu_eth.price, pu_erc20s.price
, agg.name, agg.contract_address, nft_count.nfts_minted_in_tx, pu_erc20s.symbol, erc20s.contract_address, et.success