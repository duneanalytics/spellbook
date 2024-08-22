{{ config(
    schema = 'rollup_economics_ethereum'
    , alias = 'l1_blob_fees'
    , materialized = 'view'
    , unique_key = ['name', 'tx_hash']
)}}

WITH blob_txs AS (
    SELECT 
        lower(b.blob_submitter_label) AS name
        , cast(date_trunc('month', b.block_time) AS date) AS block_month
        , cast(date_trunc('day', b.block_time) AS date) AS block_date
        , b.block_time
        , b.block_number
        , b.tx_hash
        , b.tx_index
        , b.blob_base_fee
        , b.blob_gas_used
        , (b.blob_base_fee / 1e18) * b.blob_gas_used AS blob_fee_native
        , (b.blob_base_fee / 1e18) * b.blob_gas_used * p.price AS blob_fee_usd
        , b.blob_count
    FROM {{ ref('ethereum_blob_submissions')}} b
    INNER JOIN {{ source('prices', 'usd') }} p
        ON p.minute = date_trunc('minute', b.block_time)
        AND p.blockchain IS NULL
        AND p.symbol = 'ETH'
        AND p.minute >= TIMESTAMP '2024-03-13' -- EIP-4844 launch date
    WHERE b.blob_submitter_label IN (
        'Arbitrum'
        , 'Linea'
        , 'zkSync Era'
        , 'Base'
        , 'Scroll'
        , 'Zora'
        , 'Public Goods Network'
        , 'OP Mainnet'
        , 'Starknet'
        , 'Mode'
        , 'Blast'
    )
)

SELECT
    name
    , block_month
    , block_date
    , block_time
    , block_number
    , tx_hash
    , tx_index
    , blob_base_fee
    , blob_gas_used
    , blob_fee_native
    , blob_fee_usd
    , blob_count
FROM blob_txs