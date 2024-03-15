{{ config(
    schema = 'blobs_ethereum',
    alias = 'base_fees',    
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['tx_hash', 'blob_versioned_hash'],
    post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "project",
                                    "blobs",
                                    \'["0xrob","msilb7","lorenz234"]\') }}'
)}}

WITH exponential_cte AS (
  -- Initial query, starting from each row in ethereum.blocks
  SELECT 
    blocks.number AS block_number, 
    blocks.time as block_time,
    CAST(1 AS bigint) AS i, -- Explicitly cast to bigint to match the expected type in recursion
    CAST(1 * 3338477 AS bigint) AS numerator_accum, -- Ensure the type matches through the CTE
    CAST(0 AS bigint) AS output, -- Match the output column type
    excess_blob_gas
    FROM {{ source('ethereum','blocks') }} blocks
  WHERE 
    number >= 19426587 -- First block where excess_blob_gas IS NOT NULL
    AND excess_blob_gas IS NOT NULL
  
)
SELECT 
    block_number, 
    block_time,
    max(output) AS max_output,
    greatest( max(output) / 3338477 , 1) AS blob_base_fee_per_gas,
    MAX(excess_blob_gas) AS excess_blob_gas
FROM 
    exponential_cte
GROUP BY block_number, block_time
ORDER BY block_time_cte DESC;
