{{ config(
    schema = 'blobs_ethereum',
    alias = 'blobs_l1transaction',    
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['tx_hash','blob_index','blob_kzg_commitment'],
    post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "project",
                                    "blobs",
                                    \'["msilb7","lorenz234"]\') }}'
)}}

SELECT
    b.*
    , t.block_number
    , t.hash as tx_hash
    , t."from" as tx_from
    , t.to as tx_to
    , t.gas_used as tx_gas_used
    , t.gas_price as tx_l1_gas_price
    , l.base_fee_per_gas AS tx_l1_base_fee
    , t.blob_versioned_hashes
    , t.max_fee_per_blob_gas
    , l.excess_blob_gas
    , gp.blob_base_fee as blob_base_fee_per_gas
    , CARDINALITY(t.blob_versioned_hashes) AS blobs_per_tx
FROM {{ ref('enriched') }} b
left JOIN {{ source('ethereum','blocks') }} l
    ON b.blob_parent_root = l.parent_beacon_block_root
    AND l.date >= cast('2024-03-12' as date)
    {% if is_incremental() %}
    AND {{ incremental_predicate('b.block_time')}}
    AND {{ incremental_predicate('l.time')}}
    {% endif %}
left JOIN {{ source('ethereum','transactions') }} t
    ON t.block_number = l.number 
    AND t.type = '3'
    AND contains(t.blob_versioned_hashes, b.blob_versioned_hash)
    AND t.block_date >= cast('2024-03-12' as date)
    {% if is_incremental() %}
    AND {{ incremental_predicate('b.block_time')}}
    AND {{ incremental_predicate('t.block_time')}}
    {% endif %}
LEFT JOIN {{ source('resident_wizards','dataset_blob_base_fees_lookup', database="dune") }} gp --ref. https://dune.com/queries/3521876 
    ON l.excess_blob_gas = gp.excess_blob_gas