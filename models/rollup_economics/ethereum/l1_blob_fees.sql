{{ config(
    schema = 'rollup_economics_ethereum',
    alias = 'l1_blob_fees',    
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['name', 'hash'],
    post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "project",
                                    "rollup_economics",
                                    \'["niftytable", "maybeYonas"]\') }}'
)}}

SELECT 
lower(blob_submitter_label) as name,
tx_hash as hash,
block_time,
block_number,
blob_gas_used,
blob_base_fee,
(blob_gas_used*blob_base_fee)/1e18 as blob_spend,
p.price * (blob_gas_used*blob_base_fee)/1e18 as blob_spend_usd,
blob_count
FROM {{ ref('ethereum_blob_submissions')}} s
INNER JOIN {{ source('prices','usd') }} p
ON p.minute = date_trunc('minute', s.block_time)
AND p.blockchain is null
AND p.symbol = 'ETH'
AND p.minute >= timestamp '2024-03-13'
AND s.block_time >= timestamp '2024-03-13'
{% if is_incremental() %}
AND s.block_time >= date_trunc('day', now() - interval '7' day)
AND p.minute >= date_trunc('day', now() - interval '7' day)
{% endif %}
AND s.blob_submitter_label IN ('Arbitrum',
'Linea',
'zkSync Era',
'Base',
'Scroll',
'Zora',
'Public Goods Network',
'OP Mainnet', 
'Starknet', 
'Mode',
'Blast'
)