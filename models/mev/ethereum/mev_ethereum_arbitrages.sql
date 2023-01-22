{{ config(
    alias = 'arbitrages',
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'blockchain', 'tx_hash', 'contract_address'],
    post_hook='{{ expose_spells(\'["ethereum"]\',
                                "sector",
                                "mev",
                                \'["soispoke"]\') }}'
    )
}}

with transfers_list as (SELECT el.block_time,
       el.block_number,
       el.tx_hash,
       el.contract_address as token_address,
       bytea2numeric_v2(ltrim( '0x', el.data)) AS value,
       rpad('0x',42,ltrim('0x000000000000000000000000',el.topic2)) AS transfer_from,
       rpad('0x',42,ltrim('0x000000000000000000000000',el.topic3)) AS transfer_to,
       el.index,
       CASE WHEN array_contains(collect_set(cast(tr.success as string)),'false') THEN 'false'
            ELSE 'true' END as success,
       CASE WHEN SUM(tr.value) > 0 THEN tx.from
            ELSE tx.to END AS contract_address,
       tx.from as tx_from,
       tx.to as tx_to,
       tx.index as tx_index
FROM {{ source('ethereum','logs') }} el
LEFT JOIN {{ source('ethereum','transactions') }} tx ON tx.hash = el.tx_hash AND tx.block_time = el.block_time
LEFT JOIN {{ source('ethereum','traces') }} tr ON tr.tx_hash = el.tx_hash AND tr.block_time = el.block_time
WHERE 1=1
    AND array_contains(array(
                '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'),
                topic1)
    AND rpad('0x',42,ltrim('0x000000000000000000000000',el.topic2)) not in (
        select address from {{ ref('addresses_ethereum_bridges') }}
      )
GROUP BY 1,2,3,4,5,6,7,8,tx.from,tx.to,tx.index
            UNION ALL
SELECT block_time,
       block_number,
       hash as tx_hash,
       '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' as token_address,
       value,
       from as transfer_from,
       to as transfer_to,
       0 as index,
       cast(success as string) as success,
       CASE WHEN SUM(value) > 0 THEN from
            ELSE to END AS contract_address,
       from as tx_from,
       to as tx_to,
       index as tx_index
FROM {{ source('ethereum','transactions') }}
GROUP BY 1,2,3,4,5,6,7,8,from,to,index,success
ORDER BY index ASC),

label_sandwich as (
SELECT DISTINCT 
tl0.tx_hash,
CASE WHEN array_contains(array(txm3.to,txm2.to,txm1.to,tx1.to,tx2.to,tx3.to),tl0.tx_to) THEN 'sandwich'
	   WHEN array_contains(array(txm3.from,txm2.from,txm1.from,tx1.from,tx2.from,tx3.from),tl0.tx_from) THEN 'sandwich'
     ELSE 'not_sandwich' END AS label
FROM transfers_list tl0
LEFT JOIN {{ source('ethereum','transactions') }} tx1
    ON tl0.block_time = tx1.block_time AND tl0.block_number = tx1.block_number AND tx1.index = tl0.tx_index + 1
LEFT JOIN {{ source('ethereum','transactions') }} tx2 
    ON tl0.block_time = tx2.block_time AND tl0.block_number = tx2.block_number AND tx2.index = tl0.tx_index + 2
LEFT JOIN {{ source('ethereum','transactions') }} tx3
    ON tl0.block_time = tx3.block_time AND tl0.block_number = tx3.block_number AND tx3.index = tl0.tx_index + 3
LEFT JOIN {{ source('ethereum','transactions') }} txm1
    ON tl0.block_time = txm1.block_time AND tl0.block_number = txm1.block_number AND txm1.index = tl0.tx_index - 1
LEFT JOIN {{ source('ethereum','transactions') }} txm2 
    ON tl0.block_time = txm2.block_time AND tl0.block_number = txm2.block_number AND txm2.index = tl0.tx_index - 2
LEFT JOIN {{ source('ethereum','transactions') }} txm3
    ON tl0.block_time = txm3.block_time AND tl0.block_number = txm3.block_number AND txm3.index = tl0.tx_index - 3),

label_bridge as (
SELECT tl0.tx_hash,
	   CASE WHEN transfer_from in (select address from addresses_ethereum.bridges) then 'bridge'
					  ELSE 'not_bridge' END AS label_bridge
FROM transfers_list tl0
WHERE index != 0
ORDER BY index ASC
LIMIT 1),

transfers_table AS (
SELECT tl1.block_time,
       tl1.tx_hash,
       tl1.transfer_from AS address,
       cast(-tl1.value AS DOUBLE) AS value,
       tl1.token_address,
       tl1.index,
       tl1.contract_address,
       tl1.tx_to,
       tl1.tx_from
FROM transfers_list tl1
UNION ALL
SELECT tl2.block_time,
       tl2.tx_hash,
       tl2.transfer_to AS address,
       cast(tl2.value AS DOUBLE) AS value,
       tl2.token_address,
       tl2.index,
       tl2.contract_address,
       tl2.tx_to,
       tl2.tx_from
FROM transfers_list tl2),

check_surplus AS (
SELECT tx_hash, 
       block_time, 
       SUM(sum_value_usd) AS revenue_amount_usd,
       collect_set(token_address) AS revenue_token_addresses,
       collect_set(symbol) AS revenue_token_symbols
FROM (
SELECT SUM(value/power(10,p.decimals)*p.price) AS sum_value_usd,
       tx_hash,
       block_time,
       address,
       token_address,
       p.symbol as symbol
FROM transfers_table tt
JOIN {{ source('prices','usd') }} p ON p.minute = date_trunc('minute', tt.block_time) 
    AND p.contract_address = token_address
    AND p.blockchain = 'ethereum'
WHERE address = tt.contract_address
GROUP BY 2,3,4,5,6)
GROUP BY 1,2),

count_trades AS (
SELECT tx_hash, block_time, COUNT(*) AS nb_of_trades 
FROM (SELECT SUM(value),
       block_time,
       token_address,
       tx_hash
FROM transfers_table
GROUP BY 2,3,4)
GROUP BY 1,2
)

SELECT 
       blockchain,
       block_time,
       tx_hash,
       contract_address,
       profit_amount_usd,
			 projects,
       tokens,
       revenue_amount_usd,
       revenue_token_symbols,
       cost_amount,
       cost_amount_usd,
       size(projects) as count_unique_projects,
       size(tokens) as count_unique_tokens
FROM 
(SELECT DISTINCT 
       collect_set(dex.project || '_v' || dex.version) as projects, 
       collect_set(dex.token_bought_symbol) as tokens,
       'ethereum' as blockchain,
       tl.block_time,
       tl.tx_hash,
       revenue_amount_usd,
       revenue_token_symbols,
       tl.contract_address as contract_address,
       tx_fee_native as cost_amount,
       tx_fee_usd as cost_amount_usd,
       revenue_amount_usd - tx_fee_usd as profit_amount_usd
FROM transfers_list tl
LEFT JOIN check_surplus cs ON tl.tx_hash = cs.tx_hash AND tl.block_time = cs.block_time
LEFT JOIN count_trades ct ON tl.tx_hash = ct.tx_hash AND tl.block_time = ct.block_time
LEFT JOIN label_sandwich ls ON tl.tx_hash = ls.tx_hash
LEFT JOIN label_bridge lb ON tl.tx_hash = lb.tx_hash
LEFT JOIN {{ ref('gas_ethereum_fees') }} gf ON tl.tx_hash = gf.tx_hash AND tl.block_time = gf.block_time
LEFT JOIN {{ ref('dex_trades') }} dex ON tl.tx_hash = dex.tx_hash AND tl.block_time = dex.block_time
WHERE 1=1
      AND dex.blockchain = 'ethereum'
      AND label_sandwiches = 'not_sandwich'
      AND label_bridges = 'not_bridge'
      AND cs.revenue_amount_usd > 0
      AND ct.nb_of_trades > 0
      AND tl.tx_to not in (
        select address from {{ ref('addresses_ethereum_dex') }}
      )
      AND tl.tx_from not in (
        select address from {{ ref('addresses_ethereum_dex') }}
      )
GROUP BY 3,4,5,6,7,8,9,10,11)
WHERE size(projects) > 1 AND size(tokens) > 1
ORDER BY revenue_amount_usd DESC