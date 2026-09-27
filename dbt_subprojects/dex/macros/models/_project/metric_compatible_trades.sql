{#
    Metric (oracle-based on-chain market maker) — base_trades macro.

    Methodology
    -----------
    Metric pools (MetricOmmPool) are full contracts (no proxies). Three contract generations exist; in each one a
    MetricOmmPoolFactory calls a MetricOmmPoolDeployer which CREATEs the pool. The contracts are decoded on Dune under
    project `metric` (multi-chain submission -> schema `metric_multichain`, filtered on `chain`).

    Trades: the decoded Swap events of the pool contracts (metricommpoolv{0,1,2}_evt_swap).
      version 0  Swap(sender, recipient, exactInput, int128 amount0Delta, int128 amount1Delta, newTick, newPositionInBin)
      version 1  Swap(sender, recipient, exactInput, int256 amount0Delta, int256 amount1Delta, newTick, newPositionInBin,
                      protocolFeeAmount)
      version 2  Swap(sender, recipient, uint256 details, uint256 amountDeltas, uint256 platformFees)
                 amountDeltas = (int128 amount0Delta << 128) | int128 amount1Delta (amount0 in the HIGH half, each half
                 two's-complement) -> unpacked below with uint256 division / modulo.
    Sign convention (all versions): positive delta = token moved INTO the pool (sold by the taker),
    negative delta = token moved OUT of the pool (bought by the taker).

    Token identification: token0/token1 are pool immutables and are not part of the Swap payload; they come from the
    factory's PoolCreated event.
      version 0  decoded metricommpoolfactoryv0_evt_poolcreated.
      version 1/2  PoolCreated(address indexed poolAddress, address indexed token0, address indexed token1, ... 18 more)
                 has 21 arguments, above Dune's 16-argument decoding limit, so the decoded view is not queryable
                 ("Column reference 'decoded.args_17' is invalid"). Following Dune's documented workaround, only the
                 three indexed topics are read from the factory's logs (topic1 = pool, topic2 = token0, topic3 = token1).

    Validation: decoded swap counts equal the raw Swap logs per pool on all 10 chains; pool tokens match each pool's
    on-chain getImmutables(); amounts were cross-checked against ERC20 transfers of ~215k swaps before this model was
    written (exact apart from a handful of 1-wei input roundings and 30 zero-amount swaps, filtered out).
#}

{% macro metric_compatible_trades(
    blockchain = null,
    project = 'metric',
    project_start_date = '2026-02-23',
    factories = [
        {'version': '1', 'factory': '0x622911384e7973439b8be305f5e3fc3c5736ede4'},
        {'version': '2', 'factory': '0xa32761549a1de40060c194c86a8df24f0a29ba2d'}
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

pools AS (
    SELECT '0' AS version, pool, token0, token1
    FROM {{ source('metric_multichain', 'metricommpoolfactoryv0_evt_poolcreated') }}
    WHERE chain = '{{ blockchain }}'

    UNION ALL

    SELECT
        f.version,
        bytearray_substring(l.topic1, 13, 20) AS pool,
        bytearray_substring(l.topic2, 13, 20) AS token0,
        bytearray_substring(l.topic3, 13, 20) AS token1
    FROM {{ source(blockchain, 'logs') }} l
    INNER JOIN factories f ON l.contract_address = f.factory
    WHERE l.topic0 = {{ pool_created_topic0 }}
    AND l.block_date >= DATE '{{ project_start_date }}'
),

swaps AS (
    SELECT
        '0' AS version,
        evt_block_number AS block_number,
        evt_block_time AS block_time,
        evt_tx_hash AS tx_hash,
        evt_index,
        contract_address AS pool,
        recipient,
        amount0Delta AS delta0,
        amount1Delta AS delta1
    FROM {{ source('metric_multichain', 'metricommpoolv0_evt_swap') }}
    WHERE chain = '{{ blockchain }}'
    AND evt_block_time >= TIMESTAMP '{{ project_start_date }}'
    {% if is_incremental() %}
    AND {{ incremental_predicate('evt_block_time') }}
    {% endif %}

    UNION ALL

    SELECT
        '1' AS version,
        evt_block_number,
        evt_block_time,
        evt_tx_hash,
        evt_index,
        contract_address,
        recipient,
        amount0Delta,
        amount1Delta
    FROM {{ source('metric_multichain', 'metricommpoolv1_evt_swap') }}
    WHERE chain = '{{ blockchain }}'
    AND evt_block_time >= TIMESTAMP '{{ project_start_date }}'
    {% if is_incremental() %}
    AND {{ incremental_predicate('evt_block_time') }}
    {% endif %}

    UNION ALL

    -- v2: unpack the two signed int128 halves of amountDeltas
    SELECT
        '2' AS version,
        evt_block_number,
        evt_block_time,
        evt_tx_hash,
        evt_index,
        contract_address,
        recipient,
        CASE WHEN hi >= UINT256 '170141183460469231731687303715884105728'
             THEN CAST(hi AS int256) - INT256 '340282366920938463463374607431768211456'
             ELSE CAST(hi AS int256) END,
        CASE WHEN lo >= UINT256 '170141183460469231731687303715884105728'
             THEN CAST(lo AS int256) - INT256 '340282366920938463463374607431768211456'
             ELSE CAST(lo AS int256) END
    FROM (
        SELECT
            evt_block_number, evt_block_time, evt_tx_hash, evt_index, contract_address, recipient,
            amountDeltas / UINT256 '340282366920938463463374607431768211456' AS hi,
            amountDeltas % UINT256 '340282366920938463463374607431768211456' AS lo
        FROM {{ source('metric_multichain', 'metricommpoolv2_evt_swap') }}
        WHERE chain = '{{ blockchain }}'
        AND evt_block_time >= TIMESTAMP '{{ project_start_date }}'
        {% if is_incremental() %}
        AND {{ incremental_predicate('evt_block_time') }}
        {% endif %}
    )
)

SELECT
    '{{ blockchain }}' AS blockchain,
    '{{ project }}' AS project,
    s.version,
    CAST(date_trunc('month', s.block_time) AS date) AS block_month,
    CAST(date_trunc('day', s.block_time) AS date) AS block_date,
    s.block_time,
    s.block_number,
    CAST(abs(CASE WHEN s.delta0 < 0 THEN s.delta0 ELSE s.delta1 END) AS uint256) AS token_bought_amount_raw,
    CAST(abs(CASE WHEN s.delta0 > 0 THEN s.delta0 ELSE s.delta1 END) AS uint256) AS token_sold_amount_raw,
    CASE WHEN s.delta0 < 0 THEN p.token0 ELSE p.token1 END AS token_bought_address,
    CASE WHEN s.delta0 > 0 THEN p.token0 ELSE p.token1 END AS token_sold_address,
    s.recipient AS taker,
    CAST(NULL AS varbinary) AS maker,
    s.pool AS project_contract_address,
    s.tx_hash,
    s.evt_index
FROM swaps s
INNER JOIN pools p ON p.pool = s.pool AND p.version = s.version
WHERE s.delta0 <> 0 AND s.delta1 <> 0

{% endmacro %}
