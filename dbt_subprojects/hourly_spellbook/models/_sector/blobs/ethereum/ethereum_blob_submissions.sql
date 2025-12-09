{{ config(
    schema = 'ethereum',
    alias = 'blobs_submissions',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_number','tx_index'],
    post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "project",
                                    "blobs",
                                    \'["msilb7","lorenz234","0xRob", "hildobby"]\') }}'
)}}

-- EIP-7918 (Fusaka) changes how excess_blob_gas is updated, breaking the assumption
-- that it only takes values at multiples of GAS_PER_BLOB. We must compute fake_exponential
-- in SQL for Fusaka blocks.

with blob_transactions as (
    SELECT *
    FROM {{ source('ethereum','transactions') }}
    WHERE type = '3'
    AND block_number >= 19426587 -- dencun upgrade
    {% if is_incremental() %}
    AND {{ incremental_predicate('block_time')}}
    {% endif %}
),

-- Get unique excess_blob_gas values from Fusaka blocks that need fake_exponential computed
fusaka_excess_values as (
    SELECT DISTINCT block.excess_blob_gas
    FROM blob_transactions t
    INNER JOIN {{ source('ethereum', 'blocks')}} block
        ON t.block_number = block.number
    WHERE block.number >= 23935694  -- Fusaka activation
    {% if is_incremental() %}
    AND {{ incremental_predicate('t.block_time') }}
    {% endif %}
),

-- Recursive CTE to compute fake_exponential for Fusaka
-- This implements: fake_exponential(1, excess_blob_gas, 5007716)
-- which approximates: 1 * e^(excess_blob_gas / 5007716) using Taylor expansion
fake_exponential_calc AS (
    -- Base case: initialize Taylor expansion for each unique excess_blob_gas
    SELECT 
        CAST(excess_blob_gas AS UINT256) as excess_blob_gas,
        CAST(5007716 AS UINT256) as denominator,          -- BLOB_BASE_FEE_UPDATE_FRACTION (Pectra/Fusaka)
        CAST(5007716 AS UINT256) as accum,                -- factor * denominator = 1 * 5007716
        CAST(0 AS UINT256) as output,                     -- running sum (before final division)
        1 as iteration
    FROM fusaka_excess_values
    
    UNION ALL
    
    -- Recursive case: Taylor expansion iteration
    -- Each step: output += accum, then accum = accum * numerator / denominator / i
    SELECT
        excess_blob_gas,
        denominator,
        (accum * excess_blob_gas / denominator) / CAST(iteration AS UINT256) as accum,
        output + accum as output,
        iteration + 1
    FROM fake_exponential_calc
    WHERE accum > CAST(0 AS UINT256) 
        AND iteration < 300  -- Safety limit (convergence typically happens much sooner)
),

-- Get final result: the last iteration for each excess_blob_gas
fusaka_blob_fees AS (
    SELECT 
        excess_blob_gas,
        output / denominator as blob_base_fee_exp
    FROM (
        SELECT 
            *,
            ROW_NUMBER() OVER (PARTITION BY excess_blob_gas ORDER BY iteration DESC) as rn
        FROM fake_exponential_calc
    ) ranked
    WHERE rn = 1
)

SELECT
     t.block_number
    , t.block_time
    , t.block_date
    , beacon.slot as beacon_slot_number
    , beacon.epoch as beacon_epoch
    , t.value as tx_value
    , t.hash as tx_hash
    , t."from" as blob_submitter
    , t.to as blob_receiver
    , COALESCE(l.entity, ls.entity) as blob_submitter_label
    , l.proposer as blob_proposer_label
    , t.index as tx_index
    , t.success as tx_success
    , t.data as tx_data
    , t.type as tx_type
    , t.blob_versioned_hashes
    , sequence(
        (sum(CARDINALITY(t.blob_versioned_hashes)) over (partition by t.block_number order by index asc)) - CARDINALITY(t.blob_versioned_hashes)
        ,(sum(CARDINALITY(t.blob_versioned_hashes)) over (partition by t.block_number order by index asc)) - 1
        ,1
    ) as blob_indexes
    , CARDINALITY(t.blob_versioned_hashes) AS blob_count
    , CARDINALITY(t.blob_versioned_hashes) * pow(2,17) as blob_gas_used -- within this tx
    -- Blob base fee calculation:
    -- - Dencun/Pectra: Use lookup table (excess_blob_gas moves in GAS_PER_BLOB increments)
    -- - Fusaka: Compute fake_exponential in SQL + apply EIP-7918 reserve price
    , case 
        when t.block_number < 22431084 then fee.blob_base_fee_dencun  -- Dencun
        when t.block_number < 23935694 then fee.blob_base_fee_pectra  -- Pectra
        else GREATEST(
            fusaka_fee.blob_base_fee_exp,  -- fake_exponential result computed above
            block.base_fee_per_gas * CAST(8192 AS UINT256) / CAST(131072 AS UINT256)  -- EIP-7918 reserve price (base_fee / 16)
        )
      end as blob_base_fee
    , t.max_fee_per_blob_gas
    , coalesce(("LEFT"(from_utf8(t.data), 5)='data:'), false) as is_blobscription
FROM blob_transactions t
INNER JOIN {{ source('ethereum', 'blocks')}} block
    ON t.block_number = block.number
    AND block.number >= 19426587    -- dencun upgrade
    {% if is_incremental() %}
    and {{ incremental_predicate('t.block_time') }}
    {% endif %}
INNER JOIN {{ source('beacon', 'blocks') }} beacon
    ON beacon.parent_root = block.parent_beacon_block_root
    AND beacon.slot >= 8626176 -- dencun upgrade
    {% if is_incremental() %}
    and {{ incremental_predicate('beacon.time') }}
    {% endif %}
-- Lookup table for Dencun/Pectra (only valid when excess_blob_gas is multiple of GAS_PER_BLOB)
LEFT JOIN {{ source("resident_wizards", "blob_base_fees_lookup_v2", database="dune") }} fee
    ON fee.excess_blob_gas = block.excess_blob_gas
    AND block.number < 23935694  -- Only use lookup for pre-Fusaka
-- Computed fake_exponential for Fusaka blocks
LEFT JOIN fusaka_blob_fees fusaka_fee
    ON fusaka_fee.excess_blob_gas = CAST(block.excess_blob_gas AS UINT256)
    AND block.number >= 23935694  -- Only use computed value for Fusaka
LEFT JOIN {{ref('blobs_submitters')}} l
    ON t."from" = l.address
LEFT JOIN {{ref('blobs_based_submitters')}} ls
    ON t.block_number = ls.block_number
    AND t.hash = ls.tx_hash