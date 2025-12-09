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

-- Recursive CTE to compute fake_exponential for Fusaka
-- This implements: fake_exponential(1, excess_blob_gas, 5007716)
-- which approximates: 1 * e^(excess_blob_gas / 5007716) using Taylor expansion
WITH RECURSIVE fake_exponential_calc (
    excess_blob_gas,
    i,
    accum,
    output
) AS (
    -- Non-recursive term: initialize for each unique Fusaka excess_blob_gas
    SELECT 
        blocks.excess_blob_gas,
        CAST(1 AS bigint) AS i,
        CAST(1 * 5007716 AS double) AS accum,   -- factor * denominator = 1 * 5007716
        CAST(0 AS double) AS output
    FROM {{ source('ethereum', 'blocks') }} blocks
    WHERE blocks.number >= 23935694  -- Fusaka activation
        AND blocks.excess_blob_gas IS NOT NULL
    {% if is_incremental() %}
        AND blocks.time >= (SELECT MAX(block_time) - interval '7' day FROM {{ this }})
    {% endif %}
    
    UNION ALL
    
    -- Recursive term: Taylor expansion iteration
    -- Each step: output += accum, then accum = accum * numerator / denominator / i
    SELECT
        cte.excess_blob_gas,
        cte.i + 1,
        (cte.accum * cte.excess_blob_gas) / (5007716E0 * (cte.i + 1)),  -- force double arithmetic
        cte.output + cte.accum
    FROM fake_exponential_calc cte
    WHERE cte.accum > 0
        AND cte.i < 10  -- Max recursion depth
),

-- Get final blob base fee for each unique excess_blob_gas
fusaka_blob_fees (excess_blob_gas, blob_base_fee_exp) AS (
    SELECT 
        excess_blob_gas,
        GREATEST(MAX(output) / 5007716E0, 1E0)
    FROM fake_exponential_calc
    GROUP BY excess_blob_gas
),

blob_transactions (block_number, block_time, block_date, tx_hash, tx_from, tx_to, tx_index, tx_success, tx_data, tx_type, blob_versioned_hashes, max_fee_per_blob_gas, tx_value) AS (
    SELECT block_number, block_time, block_date, hash, "from", "to", index, success, data, type, blob_versioned_hashes, max_fee_per_blob_gas, value
    FROM {{ source('ethereum','transactions') }}
    WHERE type = '3'
    AND block_number >= 19426587 -- dencun upgrade
    {% if is_incremental() %}
    AND {{ incremental_predicate('block_time')}}
    {% endif %}
)

SELECT
     t.block_number
    , t.block_time
    , t.block_date
    , beacon.slot as beacon_slot_number
    , beacon.epoch as beacon_epoch
    , t.tx_value as tx_value
    , t.tx_hash as tx_hash
    , t.tx_from as blob_submitter
    , t.tx_to as blob_receiver
    , COALESCE(l.entity, ls.entity) as blob_submitter_label
    , l.proposer as blob_proposer_label
    , t.tx_index as tx_index
    , t.tx_success as tx_success
    , t.tx_data as tx_data
    , t.tx_type as tx_type
    , t.blob_versioned_hashes
    , sequence(
        (sum(CARDINALITY(t.blob_versioned_hashes)) over (partition by t.block_number order by t.tx_index asc)) - CARDINALITY(t.blob_versioned_hashes)
        ,(sum(CARDINALITY(t.blob_versioned_hashes)) over (partition by t.block_number order by t.tx_index asc)) - 1
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
            CAST(fusaka_fee.blob_base_fee_exp AS UINT256),  -- fake_exponential result computed above
            block.base_fee_per_gas / CAST(16 AS UINT256)    -- EIP-7918 reserve price (base_fee / 16)
        )
      end as blob_base_fee
    , t.max_fee_per_blob_gas
    , coalesce(("LEFT"(from_utf8(t.tx_data), 5)='data:'), false) as is_blobscription
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
    ON fusaka_fee.excess_blob_gas = block.excess_blob_gas
    AND block.number >= 23935694  -- Only use computed value for Fusaka
LEFT JOIN {{ref('blobs_submitters')}} l
    ON t.tx_from = l.address
LEFT JOIN {{ref('blobs_based_submitters')}} ls
    ON t.block_number = ls.block_number
    AND t.tx_hash = ls.tx_hash