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

with blob_transactions as (
SELECT *
FROM {{ source('ethereum','transactions') }}
WHERE type = '3'
AND block_number >= 19426587    -- dencun upgrade
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
    , fee.blob_base_fee as blob_base_fee
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
-- this lookup relies on the invariant that the excess blob gas is updated with fixed increment and thus only ever holds a limited set of values.
LEFT JOIN  {{ source("resident_wizards", "dataset_blob_base_fees_lookup", database="dune") }} fee
    ON fee.excess_blob_gas = block.excess_blob_gas
LEFT JOIN {{ref('blobs_submitters')}} l
    ON t."from" = l.address
LEFT JOIN {{ref('blobs_based_submitters')}} ls
    ON t.block_number = ls.block_number
    AND t.hash = ls.tx_hash
