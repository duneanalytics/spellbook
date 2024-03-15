{{ config(
    schema = 'blobs_ethereum',
    alias = 'blobs_transactions',    
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['tx_hash', 'blob_versioned_hash'],
    post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "project",
                                    "blobs",
                                    \'["msilb7","lorenz234"]\') }}'
)}}


SELECT
    t.block_time
    , t.block_number
    , t.hash as tx_hash
    , t."from" as tx_from
    , t.to as tx_to
    , t.gas_used as tx_gas_used
    , t.gas_price as tx_l1_gas_price
    , l.base_fee_per_gas AS tx_l1_base_fee
    , t.blob_versioned_hashes
    , b.versioned_hash AS blob_versioned_hash
    , b.used_blob_byte_count -- commented out of blob query for now, since it's slow
    , b.blob_byte_count AS blobgas_used_by_blob
    , t.max_fee_per_blob_gas
    , l.excess_blob_gas
    , gp.blob_base_fee as blob_base_fee_per_gas
    , ROW_NUMBER() OVER (ORDER BY t.block_time ASC) AS unique_id
    , CARDINALITY(b.versioned_hash) AS num_blobs_per_tx
    --, SUM(used_blob_byte_count) AS blobgas_purchased
    --, SUM(blob_byte_count) AS blobgas_purchased
FROM (
    SELECT
        b.block_epoch
        , b.block_slot
        , b.block_time
        , b.block_date
        , b.index
        , b.proposer_index
        , b.body_root
        , b.parent_root
        , b.state_root
        -- belows expression is very slow
        ,ceil( cast(length(regexp_replace(cast(blob as varchar), '0*$', '')) - 2 as double) /2 ) AS used_blob_byte_count -- handle for last byte having a 0 at the end
        ,bytearray_length(blob) AS blob_byte_count
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
        )) as versioned_hash
        FROM {{ source('beacon','blobs') }} b 
    ) b 
left JOIN {{ source('ethereum','blocks') }} l
    ON b.parent_root = l.parent_beacon_block_root
    AND l.date >= cast('2024-03-12' as date)
left JOIN {{ source('ethereum','transactions') }} t
    ON t.block_number = l.number 
    AND t.type = '3'
    AND contains(t.blob_versioned_hashes, b.versioned_hash)
    AND t.block_date >= cast('2024-03-12' as date)
LEFT JOIN {{ source('blobs','base_fees') }} gp --ref. https://dune.com/queries/3521876
        ON l.excess_blob_gas = gp.excess_blob_gas








