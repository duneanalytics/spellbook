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

-- Blob base fee calculation across forks (EIP-4844, EIP-7691, EIP-7918, EIP-7892)
-- 
-- Fork Schedule and baseFeeUpdateFraction:
--   Dencun:  blocks 19426587 - 22431083, fraction = 3338477
--   Pectra:  blocks 22431084 - 23935693, fraction = 5007716
--   Fusaka:  blocks 23935694 - 23975795, fraction = 5007716 (same as Pectra, but non-standard excess increments)
--   BPO1:    blocks 23975796+,           fraction = 8346193 (EIP-7892 Blob Parameter Only fork)
--
-- For Fusaka+, we compute fake_exponential inline using Trino's reduce() function
-- with UINT256 arithmetic for exact integer precision matching go-ethereum.

with blob_transactions as (
    SELECT *
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
        (sum(CARDINALITY(t.blob_versioned_hashes)) over (partition by t.block_number order by t.index asc)) - CARDINALITY(t.blob_versioned_hashes)
        ,(sum(CARDINALITY(t.blob_versioned_hashes)) over (partition by t.block_number order by t.index asc)) - 1
        ,1
    ) as blob_indexes
    , CARDINALITY(t.blob_versioned_hashes) AS blob_count
    , CARDINALITY(t.blob_versioned_hashes) * pow(2,17) as blob_gas_used -- within this tx
    -- Blob base fee calculation:
    -- - Dencun/Pectra: Use v2 lookup table (excess_blob_gas moves in GAS_PER_BLOB increments)
    -- - Fusaka/BPO1+: Compute fake_exponential using transform+reduce with UINT256 + EIP-7918 reserve price
    , case 
        when t.block_number < 22431084 then fee.blob_base_fee_dencun  -- Dencun (fraction=3338477)
        when t.block_number < 23935694 then fee.blob_base_fee_pectra  -- Pectra (fraction=5007716)
        when t.block_number < 23975796 then GREATEST(
            -- Fusaka: fake_exponential with fraction=5007716
            reduce(
                transform(sequence(1, 100), i -> CAST(ROW(i, block.excess_blob_gas) AS ROW(i INTEGER, excess UINT256))),
                CAST(ROW(CAST(5007716 AS UINT256), CAST(0 AS UINT256)) AS ROW(accum UINT256, output UINT256)),
                (state, elem) -> CAST(ROW(
                    state.accum * elem.excess / CAST(5007716 AS UINT256) / CAST(elem.i AS UINT256),
                    state.output + state.accum
                ) AS ROW(accum UINT256, output UINT256)),
                state -> state.output / CAST(5007716 AS UINT256)
            ),
            block.base_fee_per_gas / CAST(16 AS UINT256)  -- EIP-7918 reserve price
        )
        else GREATEST(
            -- BPO1+: fake_exponential with fraction=8346193 (EIP-7892)
            reduce(
                transform(sequence(1, 100), i -> CAST(ROW(i, block.excess_blob_gas) AS ROW(i INTEGER, excess UINT256))),
                CAST(ROW(CAST(8346193 AS UINT256), CAST(0 AS UINT256)) AS ROW(accum UINT256, output UINT256)),
                (state, elem) -> CAST(ROW(
                    state.accum * elem.excess / CAST(8346193 AS UINT256) / CAST(elem.i AS UINT256),
                    state.output + state.accum
                ) AS ROW(accum UINT256, output UINT256)),
                state -> state.output / CAST(8346193 AS UINT256)
            ),
            block.base_fee_per_gas / CAST(16 AS UINT256)  -- EIP-7918 reserve price
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
LEFT JOIN {{ref('blobs_submitters')}} l
    ON t."from" = l.address
LEFT JOIN {{ref('blobs_based_submitters')}} ls
    ON t.block_number = ls.block_number
    AND t.hash = ls.tx_hash