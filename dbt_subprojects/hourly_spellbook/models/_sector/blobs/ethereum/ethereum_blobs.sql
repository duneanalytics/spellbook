{{ config(
    schema = 'ethereum',
    alias = 'blobs',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['beacon_slot_number','blob_index'],
    post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "project",
                                    "blobs",
                                    \'["msilb7","lorenz234","0xRob", "hildobby"]\') }}'
)}}

WITH blobs as (
SELECT
    -- this contains most columns from beacon.blobs expect "blob" which contains the raw data
    b.block_epoch AS beacon_epoch
    , b.block_slot AS beacon_slot_number
    , b.block_time as beacon_slot_time
    , b.parent_root as beacon_parent_root
    , b.index AS blob_index
    , b.proposer_index AS beacon_proposer_index
    , b.kzg_commitment AS blob_kzg_commitment
    ,bytearray_length(varbinary_ltrim(varbinary_rtrim(blob))) as used_blob_byte_count
    ,bytearray_length(blob) AS blob_byte_count
    ,bytearray_length(blob) AS blob_gas_used
    -- GPT to the rescue
    ,from_hex('01' || -- Prepending '01' to indicate the version byte
    substr( -- Take the substring representing the last 31 bytes of the SHA256 hash
        to_hex( -- Convert the SHA256 hash back to hexadecimal
            sha256( -- Compute the SHA256 hash of the KZG commitment
                from_hex( -- Convert the KZG commitment from hex (excluding '0x') to binary
                    substr(cast(kzg_commitment as varchar), 3)
                )
            )
        ),
        length(to_hex(sha256(from_hex(substr(cast(kzg_commitment as varchar), 3))))) - 62 + 1 -- Calculate the start position for the last 31 bytes
    )) as blob_versioned_hash
FROM {{ source('beacon','blobs') }} b
{% if is_incremental() %}
WHERE {{ incremental_predicate('b.block_time') }}
{% endif %}
)

SELECT
    b.*
    ,b_tx.block_number
    ,b_tx.tx_hash
    ,b_tx.blob_submitter
    ,b_tx.blob_submitter_label
    ,b_tx.blob_base_fee
    ,b_tx.max_fee_per_blob_gas
    ,b_tx.is_blobscription
FROM blobs b
INNER JOIN {{ ref('ethereum_blob_submissions')}} b_tx
    ON b_tx.beacon_slot_number = b.beacon_slot_number
    AND contains(b_tx.blob_versioned_hashes, b.blob_versioned_hash)
    AND contains(b_tx.blob_indexes, b.blob_index)
    {% if is_incremental() %}
    AND {{ incremental_predicate('b_tx.block_time') }}
    {% endif %}
