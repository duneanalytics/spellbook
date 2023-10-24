{{ config(
        
        schema = 'nft_ethereum',
        alias = 'native_mints',
        partition_by = ['block_month'],
		materialized = 'incremental',
		file_format = 'delta',
		incremental_strategy = 'merge',
        unique_key = ['tx_hash','evt_index','token_id','number_of_items']
        )
}}

WITH namespaces AS (
    SELECT address
    , min_by(namespace, created_at) AS namespace
	FROM {{ source('ethereum','contracts') }}
	GROUP BY address
	)

, nfts_per_tx_tmp AS (
    SELECT tx_hash
    , sum(amount) AS nfts_minted_in_tx
    FROM {{ ref('nft_ethereum_transfers') }}
    {% if is_incremental() %}
    WHERE block_time >= date_trunc('day', now() - interval '7' Day)
    {% endif %}
    GROUP BY tx_hash
    )

, nfts_per_tx as (
    SELECT
        tx_hash
        , case when nfts_minted_in_tx = UINT256 '0' THEN UINT256 '1' ELSE nfts_minted_in_tx END as nfts_minted_in_tx
    FROM
    nfts_per_tx_tmp
)
SELECT
blockchain
, project
, version
, block_time
, block_date
, block_month
, block_number
, token_id
, collection
, token_standard
, trade_type
, number_of_items
, trade_category
, evt_type
, seller
, buyer
, amount_raw
, amount_original
, amount_usd
, currency_symbol
, currency_contract
, nft_contract_address
, project_contract_address
, aggregator_name
, aggregator_address
, tx_hash
, tx_from
, tx_to
, platform_fee_amount_raw
, platform_fee_amount
, platform_fee_amount_usd
, platform_fee_percentage
, royalty_fee_receive_address
, royalty_fee_currency_symbol
, royalty_fee_amount_raw
, royalty_fee_amount
, royalty_fee_amount_usd
, royalty_fee_percentage
, evt_index
FROM
(
SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY tx_hash, evt_index, token_id, number_of_items ORDER BY amount_usd DESC NULLS LAST) as rank_index
FROM
(
SELECT distinct 'ethereum' AS blockchain
, COALESCE(ec.namespace, 'Unknown') AS project
, '' AS version
, nft_mints.block_time AS block_time
, CAST(date_trunc('day', nft_mints.block_time) as date) AS block_date
, CAST(date_trunc('month', nft_mints.block_time) as date) AS block_month
, nft_mints.block_number AS block_number
, nft_mints.token_id AS token_id
, tok.name AS collection
, nft_mints.token_standard
, CASE WHEN nft_mints.amount= UINT256 '1' THEN 'Single Item Mint'
    ELSE 'Bundle Mint'
    END AS trade_type
, nft_mints.amount AS number_of_items
, 'Mint' AS trade_category
, 'Mint' AS evt_type
, nft_mints."from" AS seller
, nft_mints.to AS buyer
, CAST(COALESCE(SUM(CAST(et.value as DOUBLE)), SUM(CAST(erc20s.value as DOUBLE)), 0)*(nft_mints.amount/nft_count.nfts_minted_in_tx) AS UINT256) AS amount_raw
, COALESCE(SUM(CAST(et.value as DOUBLE))/POWER(10, 18), SUM(CAST(erc20s.value as DOUBLE))/POWER(10, pu_erc20s.decimals))*(nft_mints.amount/nft_count.nfts_minted_in_tx) AS amount_original
, COALESCE(pu_eth.price*SUM(CAST(et.value as DOUBLE))/POWER(10, 18), pu_erc20s.price*SUM(CAST(erc20s.value as DOUBLE))/POWER(10, pu_erc20s.decimals))*(nft_mints.amount/nft_count.nfts_minted_in_tx) AS amount_usd
, CASE WHEN et.success THEN 'ETH' ELSE pu_erc20s.symbol END AS currency_symbol
, CASE WHEN et.success THEN 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2 ELSE erc20s.contract_address END AS currency_contract
, nft_mints.contract_address AS nft_contract_address
, etxs.to AS project_contract_address
, agg.name AS aggregator_name
, agg.contract_address AS aggregator_address
, nft_mints.tx_hash AS tx_hash
, etxs."from" AS tx_from
, etxs.to AS tx_to
, UINT256 '0' AS platform_fee_amount_raw
, DOUBLE '0' AS platform_fee_amount
, DOUBLE '0' AS platform_fee_amount_usd
, DOUBLE '0' AS platform_fee_percentage
, CAST(NULL as VARBINARY) AS royalty_fee_receive_address
, '0' AS royalty_fee_currency_symbol
, UINT256 '0' AS royalty_fee_amount_raw
, DOUBLE '0' AS royalty_fee_amount
, DOUBLE '0' AS royalty_fee_amount_usd
, DOUBLE '0' AS royalty_fee_percentage
, nft_mints.evt_index
FROM {{ ref('nft_ethereum_transfers') }} nft_mints
LEFT JOIN nfts_per_tx nft_count ON nft_count.tx_hash=nft_mints.tx_hash
LEFT JOIN {{ source('ethereum','traces') }} et ON et.block_time=nft_mints.block_time
    AND et.tx_hash=nft_mints.tx_hash
    AND et."from"=nft_mints.to
    AND (et.call_type NOT IN ('delegatecall', 'callcode', 'staticcall') OR et.call_type IS NULL)
    AND et.success
    AND CAST(et.value as DOUBLE) > 0
    {% if is_incremental() %}
    AND  et.block_time >= date_trunc('day', now() - interval '7' Day)
    {% endif %}
LEFT JOIN {{ source('prices','usd') }} pu_eth ON pu_eth.blockchain='ethereum'
    AND pu_eth.minute=date_trunc('minute', et.block_time)
    AND pu_eth.contract_address= 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2
    {% if is_incremental() %}
    AND  pu_eth.minute >= date_trunc('day', now() - interval '7' Day)
    {% endif %}
LEFT JOIN {{ source('erc20_ethereum','evt_transfer') }} erc20s ON erc20s.evt_block_time=nft_mints.block_time
    AND erc20s."from"=nft_mints.to
    AND erc20s.evt_tx_hash = nft_mints.tx_hash
    AND (et.value IS NULL OR CAST(et.value as double) = 0)
    {% if is_incremental() %}
    AND  erc20s.evt_block_time >= date_trunc('day', now() - interval '7' Day)
    {% endif %}
LEFT JOIN {{ source('prices','usd') }} pu_erc20s ON pu_erc20s.blockchain='ethereum'
    AND pu_erc20s.minute=date_trunc('minute', erc20s.evt_block_time)
    AND erc20s.contract_address=pu_erc20s.contract_address
    {% if is_incremental() %}
    AND  pu_erc20s.minute >= date_trunc('day', now() - interval '7' Day)
    {% endif %}
LEFT JOIN {{ source('ethereum','transactions') }} etxs ON etxs.block_time=nft_mints.block_time
    AND etxs.hash=nft_mints.tx_hash
    {% if is_incremental() %}
    AND  etxs.block_time >= date_trunc('day', now() - interval '7' Day)
    {% endif %}
LEFT JOIN {{ ref('nft_ethereum_aggregators') }} agg ON etxs.to=agg.contract_address
LEFT JOIN {{ ref('tokens_ethereum_nft') }} tok ON tok.contract_address=nft_mints.contract_address
LEFT JOIN namespaces ec ON etxs.to=ec.address
{% if is_incremental() %}
LEFT JOIN {{this}} anti_txs ON anti_txs.block_time=nft_mints.block_time
    AND anti_txs.tx_hash=nft_mints.tx_hash
WHERE anti_txs.tx_hash IS NULL
{% else %}
WHERE 1=1
{% endif %}
AND nft_mints."from"= 0x0000000000000000000000000000000000000000
AND nft_mints.contract_address NOT IN (SELECT address FROM {{ ref('addresses_ethereum_defi') }})
{% if is_incremental() %}
AND nft_mints.block_time >= date_trunc('day', now() - interval '7' Day)
{% endif %}
GROUP BY nft_mints.block_time, nft_mints.block_number, nft_mints.token_id, nft_mints.token_standard
, nft_mints.amount, nft_mints."from", nft_mints.to, nft_mints.contract_address, etxs.to, nft_mints.evt_index
, nft_mints.tx_hash, etxs."from", ec.namespace, tok.name, pu_erc20s.decimals, pu_eth.price, pu_erc20s.price
, agg.name, agg.contract_address, nft_count.nfts_minted_in_tx, pu_erc20s.symbol, erc20s.contract_address, et.success
) tmp
) tmp_2
WHERE rank_index = 1
