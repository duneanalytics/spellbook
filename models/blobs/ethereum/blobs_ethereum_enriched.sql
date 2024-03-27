{{ config(
    schema = 'blobs_ethereum_enriched',
    alias = 'blobs_ethereum_enriched',    
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['blob_block_slot','blob_index','blob_kzg_commitment'],
    post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "project",
                                    "blobs",
                                    \'["msilb7","lorenz234"]\') }}'
)}}

SELECT
    b.block_epoch AS blob_block_epoch
    , b.block_slot AS blob_block_slot
    , b.block_time
    , b.index AS blob_index
    , b.proposer_index AS blob_proposer_index
    , b.kzg_commitment AS blob_kzg_commitment
    , b.kzg_commitment_inclusion_proof AS blob_kzg_commitment_inclusion_proof
    , b.kzg_proof AS blob_kzg_proof
    , b.body_root AS blob_body_root
    , b.parent_root AS blob_parent_root
    , b.state_root AS blob_state_root
    , b.signature AS blob_signature
    -- belows expression is very slow
    ,ceil( cast(length(regexp_replace(cast(blob as varchar), '0*$', '')) - 2 as double) /2 ) AS used_blob_byte_count -- handle for last byte having a 0 at the end
    ,bytearray_length(blob) AS blobgas_used_by_blob
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
