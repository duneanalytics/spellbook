{{ config(
    schema = 'rollup_economics_ethereum'
    , alias = 'l1_verification_fees'
    , materialized = 'view'
    , unique_key = ['name', 'tx_hash']
)}}

{% set base_models = [
    ref('rollup_economics_linea_l1_verification_fees')
    , ref('rollup_economics_scroll_l1_verification_fees')
    , ref('rollup_economics_starknet_l1_verification_fees')
    , ref('rollup_economics_polygon_zkevm_l1_verification_fees')
    , ref('rollup_economics_zksync_l1_verification_fees')
] %}

WITH base_union AS (
    SELECT *
    FROM (
        {% for base_model in base_models %}
        SELECT
            name
            , block_month
            , block_date
            , block_time
            , block_number
            , tx_hash
            , tx_index
            , gas_price
            , gas_used
            , verification_fee_native
            , calldata_gas_used
            , proof_size_mb
        FROM 
            {{ base_model }}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
        {% endfor %}
    )
)

SELECT
    b.name
    , b.block_month
    , b.block_date
    , b.block_time
    , b.block_number
    , b.tx_hash
    , b.tx_index
    , b.gas_price
    , b.gas_used
    , b.verification_fee_native
    , b.verification_fee_native * p.price AS verification_fee_usd
    , b.calldata_gas_used
    , b.proof_size_mb
FROM base_union b
INNER JOIN {{ source('prices', 'usd') }} p
    ON p.minute = date_trunc('minute', b.block_time)
    AND p.blockchain IS NULL
    AND p.symbol = 'ETH'
    AND p.minute >= TIMESTAMP '2021-05-29'