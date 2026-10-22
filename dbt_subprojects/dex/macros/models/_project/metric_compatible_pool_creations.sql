{#
    Metric — pool creations.

    Why this is parsed from raw logs
    --------------------------------
    PoolCreated carries 21 arguments from version 1 onwards, above Dune's 16-argument decoding limit, so every
    decoded metricommpoolfactoryv{1,2,2_beta}_evt_poolcreated view fails to analyse with
    "Column reference 'decoded.args_17' is invalid" — the view cannot be selected from at all, not even for
    evt_tx_hash. The event is therefore read from the chain's logs table and unpacked here.

    Versions 1, 2 and 2_beta share one event signature, hence one topic0:
        PoolCreated(address indexed poolAddress, address indexed token0, address indexed token1, uint256 poolIdx,
                    address factory, address admin, address priceProvider, address[] extensions,
                    (uint256,uint256,uint256,uint256,uint256,uint256) extensionOrders, uint256 priceProviderTimelock,
                    uint256 initialScaledAmount0PerE18Shares, uint256 initialScaledAmount1PerE18Shares,
                    uint256 minimalOperationalLiquidity, uint24 spreadProtocolFeeE6, uint24 protocolNotionalFeeE8,
                    uint24 adminSpreadFeeE6, uint24 adminNotionalFeeE8, address adminFeeDestination,
                    int24 curBinDistFromProvidedPriceE6, uint256[] nonNegativeBinDataArray,
                    uint256[] negativeBinDataArray)
      -> 0x4b36a0ddce54edb36597ee7d496df06c53fe875aba9d7257534a38d5177899aa

    Head layout. extensionOrders is a STATIC tuple of six uint256, so it is encoded inline and occupies head slots
    5-10 rather than a single offset slot; every field after it sits five slots later than its argument index would
    suggest. The head is 23 slots (736 bytes).
        slot  0  poolIdx                           slot 15  spreadProtocolFeeE6
        slot  1  factory                           slot 16  protocolNotionalFeeE8
        slot  2  admin                             slot 17  adminSpreadFeeE6
        slot  3  priceProvider                     slot 18  adminNotionalFeeE8
        slot  4  extensions            -> tail     slot 19  adminFeeDestination
        slots 5-10  extensionOrders    inline      slot 20  curBinDistFromProvidedPriceE6
        slot 11  priceProviderTimelock             slot 21  nonNegativeBinDataArray -> tail
        slot 12  initialScaledAmount0PerE18Shares  slot 22  negativeBinDataArray    -> tail
        slot 13  initialScaledAmount1PerE18Shares
        slot 14  minimalOperationalLiquidity
    The three dynamic arrays hold offsets into the tail and are not expanded here; they are per-bin liquidity
    seeding data, not pool identity, and remain available from the logs table if ever needed.

    Version 0 is a different, five-argument event that decodes normally, so it is read from its decoded table. It has
    no factory-level fee or admin configuration, so those columns are null for version 0.

    Verification: the factory address read out of head slot 1 equals the contract_address that emitted the log on
    every row sampled, and poolIdx is sequential per factory.
#}

{% macro metric_compatible_pool_creations(
    blockchain = null,
    project = 'metric',
    project_start_date = '2026-02-23',
    factories = [
        {'version': '1', 'factory': '0x622911384e7973439b8be305f5e3fc3c5736ede4'},
        {'version': '2', 'factory': '0xa32761549a1de40060c194c86a8df24f0a29ba2d'},
        {'version': '2_beta', 'factory': '0x2a53833cc95548cf52c7b159110e22d3a9018f32'}
    ],
    pool_created_topic0 = '0x4b36a0ddce54edb36597ee7d496df06c53fe875aba9d7257534a38d5177899aa'
    )
%}

WITH factories AS (
    SELECT version, factory FROM (VALUES
        {% for f in factories %}
        ('{{ f["version"] }}', {{ f["factory"] }}){% if not loop.last %},{% endif %}
        {% endfor %}
    ) t(version, factory)
),

-- version 0: five-argument event, decodes normally
v0 AS (
    SELECT
        '0' AS version,
        contract_address AS factory_address,
        evt_block_time AS creation_block_time,
        evt_block_number AS creation_block_number,
        evt_tx_hash AS creation_tx_hash,
        evt_index,
        pool,
        token0,
        token1,
        CAST(NULL AS uint256) AS pool_idx,
        CAST(NULL AS varbinary) AS admin,
        priceprovider AS price_provider,
        CAST(NULL AS uint256) AS price_provider_timelock,
        CAST(NULL AS uint256) AS initial_scaled_amount0_per_e18_shares,
        CAST(NULL AS uint256) AS initial_scaled_amount1_per_e18_shares,
        CAST(NULL AS uint256) AS minimal_operational_liquidity,
        CAST(NULL AS uint256) AS spread_protocol_fee_e6,
        CAST(NULL AS uint256) AS protocol_notional_fee_e8,
        CAST(NULL AS uint256) AS admin_spread_fee_e6,
        CAST(NULL AS uint256) AS admin_notional_fee_e8,
        CAST(NULL AS varbinary) AS admin_fee_destination,
        CAST(NULL AS int256) AS cur_bin_dist_from_provided_price_e6
    FROM {{ source('metric_multichain', 'metricommpoolfactoryv0_evt_poolcreated') }}
    WHERE chain = '{{ blockchain }}'
    AND evt_block_time >= TIMESTAMP '{{ project_start_date }}'
    {% if is_incremental() %}
    AND {{ incremental_predicate('evt_block_time') }}
    {% endif %}
),

-- versions 1 / 2 / 2_beta: 21 arguments, unpacked from the log payload
v1_plus AS (
    SELECT
        f.version,
        l.contract_address AS factory_address,
        l.block_time AS creation_block_time,
        l.block_number AS creation_block_number,
        l.tx_hash AS creation_tx_hash,
        l.index AS evt_index,
        bytearray_substring(l.topic1, 13, 20) AS pool,
        bytearray_substring(l.topic2, 13, 20) AS token0,
        bytearray_substring(l.topic3, 13, 20) AS token1,
        bytearray_to_uint256(bytearray_substring(l.data, 1, 32)) AS pool_idx,
        bytearray_substring(l.data, 1 + 32 * 2 + 12, 20) AS admin,
        bytearray_substring(l.data, 1 + 32 * 3 + 12, 20) AS price_provider,
        bytearray_to_uint256(bytearray_substring(l.data, 1 + 32 * 11, 32)) AS price_provider_timelock,
        bytearray_to_uint256(bytearray_substring(l.data, 1 + 32 * 12, 32)) AS initial_scaled_amount0_per_e18_shares,
        bytearray_to_uint256(bytearray_substring(l.data, 1 + 32 * 13, 32)) AS initial_scaled_amount1_per_e18_shares,
        bytearray_to_uint256(bytearray_substring(l.data, 1 + 32 * 14, 32)) AS minimal_operational_liquidity,
        bytearray_to_uint256(bytearray_substring(l.data, 1 + 32 * 15, 32)) AS spread_protocol_fee_e6,
        bytearray_to_uint256(bytearray_substring(l.data, 1 + 32 * 16, 32)) AS protocol_notional_fee_e8,
        bytearray_to_uint256(bytearray_substring(l.data, 1 + 32 * 17, 32)) AS admin_spread_fee_e6,
        bytearray_to_uint256(bytearray_substring(l.data, 1 + 32 * 18, 32)) AS admin_notional_fee_e8,
        bytearray_substring(l.data, 1 + 32 * 19 + 12, 20) AS admin_fee_destination,
        -- int24 sign-extended across the word: read the low three bytes and undo two's complement
        CASE WHEN bytearray_to_uint256(bytearray_substring(l.data, 1 + 32 * 20 + 29, 3)) >= UINT256 '8388608'
             THEN CAST(bytearray_to_uint256(bytearray_substring(l.data, 1 + 32 * 20 + 29, 3)) AS int256)
                  - INT256 '16777216'
             ELSE CAST(bytearray_to_uint256(bytearray_substring(l.data, 1 + 32 * 20 + 29, 3)) AS int256)
        END AS cur_bin_dist_from_provided_price_e6
    FROM {{ source(blockchain, 'logs') }} l
    INNER JOIN factories f ON l.contract_address = f.factory
    WHERE l.topic0 = {{ pool_created_topic0 }}
    AND l.block_date >= DATE '{{ project_start_date }}'
    {% if is_incremental() %}
    AND {{ incremental_predicate('l.block_time') }}
    {% endif %}
),

pool_creations AS (
    SELECT * FROM v0
    UNION ALL
    SELECT * FROM v1_plus
)

SELECT
    '{{ blockchain }}' AS blockchain,
    '{{ project }}' AS project,
    version,
    factory_address,
    CAST(date_trunc('month', creation_block_time) AS date) AS creation_block_month,
    creation_block_time,
    creation_block_number,
    creation_tx_hash,
    evt_index,
    pool,
    token0,
    token1,
    pool_idx,
    admin,
    price_provider,
    price_provider_timelock,
    initial_scaled_amount0_per_e18_shares,
    initial_scaled_amount1_per_e18_shares,
    minimal_operational_liquidity,
    spread_protocol_fee_e6,
    protocol_notional_fee_e8,
    admin_spread_fee_e6,
    admin_notional_fee_e8,
    admin_fee_destination,
    cur_bin_dist_from_provided_price_e6
FROM pool_creations

{% endmacro %}
