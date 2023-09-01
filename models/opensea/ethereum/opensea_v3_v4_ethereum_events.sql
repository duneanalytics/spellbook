{{ config(
    schema = 'opensea_v3_v4_ethereum',
    alias = alias('events'),
    tags = ['dunesql'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_number', 'tx_hash', 'evt_index', 'sub_idx'],
    partition_by = ['block_date']
    )
}}

SELECT 'ethereum' AS blockchain
, date_trunc('day', s.block_time) AS block_date
, 'opensea' AS project
, CASE WHEN s.seaport_version IN ('1.1', '1.2', '1.3') THEN 'v3' ELSE 'v4' END AS version
, s.block_number
, s.tx_hash
, s.evt_index
, ROW_NUMBER() OVER (PARTITION BY s.block_number, s.tx_hash, s.evt_index ORDER BY s.order_hash) AS sub_idx
, s.seaport_contract_address AS project_contract_address
, SUM(COALESCE(s.amount, UINT256 '0')) FILTER (WHERE s.token_standard IN ('native', 'erc20')) AS amount_raw
, SUM(COALESCE(s.amount, UINT256 '1')) FILTER (WHERE s.token_standard IN ('erc721', 'erc1155')) AS number_of_items
, MIN_BY(s.token_standard, s.trace_index) FILTER (WHERE s.token_standard IN ('erc721', 'erc1155')) AS token_standard
, MIN_BY(s.token_address, s.trace_index) FILTER (WHERE s.token_standard IN ('erc721', 'erc1155')) AS nft_contract_address
, MIN_BY(s.identifier, s.trace_index) FILTER (WHERE s.token_standard IN ('erc721', 'erc1155')) AS token_id
, CASE WHEN MIN(CASE WHEN s.token_standard IN ('erc721', 'erc1155') THEN s.trace_side END) = 'consideration' THEN 'Accepted Offer' ELSE 'Buy' END AS trade_category
, MIN_BY(s.token_address, s.trace_index) FILTER (WHERE s.token_standard IN ('native', 'erc20', 'bep20')) AS currency_contract
, COALESCE(SUM(COALESCE(CAST(amount AS double), 0)) FILTER (WHERE s.recipient IN (0x8de9c5a032463c561423387a9648c5c7bcc5bc90, 0x34ba0f2379bf9b81d09f7259892e26a8b0885095, 0x0000a26b00c1f0df003000390027140000faa719)), 0) AS platform_fee_amount_raw
, MAX(s.recipient) FILTER (WHERE s.recipient IN (0x8de9c5a032463c561423387a9648c5c7bcc5bc90, 0x34ba0f2379bf9b81d09f7259892e26a8b0885095, 0x0000a26b00c1f0df003000390027140000faa719)) AS platform_fee_address
, COALESCE(SUM(COALESCE(s.amount, UINT256 '0')) FILTER (WHERE s.token_standard IN ('native', 'erc20') AND s.recipient NOT IN ( 0x8de9c5a032463c561423387a9648c5c7bcc5bc90, 0x34ba0f2379bf9b81d09f7259892e26a8b0885095, 0x0000a26b00c1f0df003000390027140000faa719)), UINT256 '0') - MAX(amount) FILTER (WHERE s.token_standard IN ('native', 'erc20')) AS royalty_fee_amount_raw
, (ARRAY_AGG(CASE WHEN s.token_standard IN ('native', 'erc20') THEN s.recipient ELSE NULL END ORDER BY CASE WHEN s.token_standard IN ('native', 'erc20') AND s.recipient NOT IN ( 0x8de9c5a032463c561423387a9648c5c7bcc5bc90, 0x34ba0f2379bf9b81d09f7259892e26a8b0885095, 0x0000a26b00c1f0df003000390027140000faa719) THEN CAST(s.amount AS double) ELSE NULL END DESC))[2] AS royalty_fee_address
, MAX(s.offerer) FILTER (WHERE s.token_standard IN ('erc721', 'erc1155')) AS buyer
, MAX(s.recipient) FILTER (WHERE s.token_standard IN ('erc721', 'erc1155')) AS seller
FROM {{ ref('seaport_ethereum_traces') }} s
INNER JOIN {{ source('ethereum', 'transactions') }} txs ON txs.block_number=s.block_number
    AND txs.hash=s.tx_hash
    {% if is_incremental() %}
    AND txs.block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
WHERE (s.zone IN (SELECT identifier FROM seaport_ethereum.tagging WHERE protocol='OpenSea' AND tagging_method='zone')
OR "RIGHT"(CAST(txs.data AS varchar), 8) = '360c6ebe')
{% if is_incremental() %}
AND s.block_time >= date_trunc('day', now() - interval '7' day)
{% endif %}
GROUP BY s.block_time, s.block_number, s.tx_hash, s.evt_index, s.order_hash, s.seaport_contract_address, s.seaport_version
HAVING MAX(CASE WHEN s.token_standard IN ('erc721', 'erc1155') AND s.trace_side='consideration' THEN true ELSE false END) != MAX(CASE WHEN s.token_standard IN ('erc721', 'erc1155') AND s.trace_side='offer' THEN true ELSE false END) -- Checks there's only an/multiple NFT(s) on one side
AND MAX(CASE WHEN s.token_standard IN ('native', 'erc20') AND s.trace_side='consideration' THEN true ELSE false END) != MAX(CASE WHEN s.token_standard IN ('native', 'erc20') AND s.trace_side='offer' THEN true ELSE false END) -- Checks there's only an/multiple native tokens/erc20s on one side
