{#
    Metric (oracle-based on-chain market maker) — base_trades macro.

    Methodology
    -----------
    Metric pools (MetricOmmPool) are full contracts (no proxies). Three contract generations exist; in each one a
    MetricOmmPoolFactory calls a MetricOmmPoolDeployer which CREATEs the pool. None of the contracts are verified on
    public explorers, so this model reads raw logs and decodes the Swap payloads directly. Amounts were cross-checked
    1:1 against the ERC20 Transfer logs of ~215k swaps across 10 chains before this model was written.

    Pool discovery: creation_traces where "from" = MetricOmmPoolDeployer of that generation (the deployer address is
    identical on every chain). Generation -> deployer / factory:
      0  deployer 0x0f732971a3503afee276782173170d1f1154d4c1  factory 0xe22f9fc0f04486de25ed6cf1800a4a47afd82e0c
      1  deployer 0x47d5c9df5e3419217471a9d12d932dbed7b0b7f1  factory 0x622911384e7973439b8be305f5e3fc3c5736ede4
      2  deployer 0x8b25c2c3dc0c3a9dc1272fbbc09e064015097a5b  factory 0xa32761549a1de40060c194c86a8df24f0a29ba2d

    Swap event layouts (pool contract emits them):
      version 0  topic0 0xcd8b75a7…  Swap(address sender, address recipient, bool exactInput, int128 amount0Delta,
                                         int128 amount1Delta, int16 newTick, uint104 newPositionInBin)   -- nothing indexed
      version 1  topic0 0x87d25816…  Swap(address indexed sender, address indexed recipient, bool exactInput,
                                         int256 amount0Delta, int256 amount1Delta, int8 newTick, uint104 newPositionInBin,
                                         uint256 protocolFeeAmount)
      version 2  topic0 0x97348197…  Swap(address indexed sender, address indexed recipient, uint256, uint256, uint256)
                                     word 1 = (int128 amount0Delta << 128) | int128 amount1Delta (amount0 in the HIGH half)
    Sign convention (all versions): positive delta = token moved INTO the pool (sold by the taker),
    negative delta = token moved OUT of the pool (bought by the taker).

    Token identification: the Swap payload carries no token addresses and token0/token1 are pool immutables
    (no creation event in v1/v2). We therefore attach, per swap, the ERC20 Transfer INTO the pool and OUT of the
    pool that immediately precede the Swap log in the same transaction. Every Metric swap moves exactly one token
    in and one token out, and pools are never nested, so this mapping is exact.
#}

{% macro metric_compatible_trades(
    blockchain = null,
    project = 'metric',
    project_start_date = '2026-02-23',
    deployers = [
        {'version': '0', 'deployer': '0x0f732971a3503afee276782173170d1f1154d4c1', 'topic0': '0xcd8b75a7fb6cb82ab3acded68ac53af5c43d19b51a91b9d5640bb73622dbdf57'},
        {'version': '1', 'deployer': '0x47d5c9df5e3419217471a9d12d932dbed7b0b7f1', 'topic0': '0x87d25816ca01843f551b4caa5eea03b5173c84c383573c63081fb7575378276e'},
        {'version': '2', 'deployer': '0x8b25c2c3dc0c3a9dc1272fbbc09e064015097a5b', 'topic0': '0x9734819749a91fc3be03ea83205f924ee08479bd3f0da48efc91d94d050cac1e'}
    ]
    )
%}

WITH deployers AS (
    SELECT version, deployer, topic0 FROM (VALUES
        {% for d in deployers %}
        ('{{ d["version"] }}', {{ d["deployer"] }}, {{ d["topic0"] }}){% if not loop.last %},{% endif %}
        {% endfor %}
    ) t(version, deployer, topic0)
),

pools AS (
    SELECT d.version, d.topic0, ct.address AS pool
    FROM {{ source(blockchain, 'creation_traces') }} ct
    INNER JOIN deployers d ON ct."from" = d.deployer
    WHERE ct.block_time >= TIMESTAMP '{{ project_start_date }}'
),

swaps AS (
    SELECT
        p.version,
        l.block_number,
        l.block_time,
        l.tx_hash,
        l.index AS evt_index,
        l.contract_address AS pool,
        CASE p.version
            WHEN '0' THEN bytearray_substring(l.data, 13, 20)       -- v0: sender not indexed
            ELSE bytearray_substring(l.topic1, 13, 20) END AS sender,
        CASE p.version
            WHEN '0' THEN bytearray_substring(l.data, 45, 20)       -- v0: recipient not indexed
            ELSE bytearray_substring(l.topic2, 13, 20) END AS recipient,
        -- signed pool delta of token0 (amount0Delta)
        CASE p.version
            WHEN '0' THEN bytearray_to_int256(bytearray_substring(l.data, 97, 32))
            WHEN '1' THEN bytearray_to_int256(bytearray_substring(l.data, 33, 32))
            WHEN '2' THEN CASE WHEN bytearray_to_uint256(bytearray_substring(l.data, 33, 16)) >= UINT256 '170141183460469231731687303715884105728'
                               THEN CAST(bytearray_to_uint256(bytearray_substring(l.data, 33, 16)) AS int256) - INT256 '340282366920938463463374607431768211456'
                               ELSE CAST(bytearray_to_uint256(bytearray_substring(l.data, 33, 16)) AS int256) END
        END AS delta0,
        -- signed pool delta of token1 (amount1Delta)
        CASE p.version
            WHEN '0' THEN bytearray_to_int256(bytearray_substring(l.data, 129, 32))
            WHEN '1' THEN bytearray_to_int256(bytearray_substring(l.data, 65, 32))
            WHEN '2' THEN CASE WHEN bytearray_to_uint256(bytearray_substring(l.data, 49, 16)) >= UINT256 '170141183460469231731687303715884105728'
                               THEN CAST(bytearray_to_uint256(bytearray_substring(l.data, 49, 16)) AS int256) - INT256 '340282366920938463463374607431768211456'
                               ELSE CAST(bytearray_to_uint256(bytearray_substring(l.data, 49, 16)) AS int256) END
        END AS delta1
    FROM {{ source(blockchain, 'logs') }} l
    INNER JOIN pools p ON l.contract_address = p.pool AND l.topic0 = p.topic0
    WHERE l.block_time >= TIMESTAMP '{{ project_start_date }}'
    {% if is_incremental() %}
    AND {{ incremental_predicate('l.block_time') }}
    {% endif %}
),

-- ERC20 transfers touching a Metric pool
transfers AS (
    SELECT
        l.tx_hash,
        l.index,
        l.contract_address AS token,
        bytearray_substring(l.topic1, 13, 20) AS "from",
        bytearray_substring(l.topic2, 13, 20) AS "to",
        bytearray_to_uint256(l.data) AS amount
    FROM {{ source(blockchain, 'logs') }} l
    WHERE l.topic0 = 0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef
    AND l.block_time >= TIMESTAMP '{{ project_start_date }}'
    {% if is_incremental() %}
    AND {{ incremental_predicate('l.block_time') }}
    {% endif %}
    AND (bytearray_substring(l.topic1, 13, 20) IN (SELECT pool FROM pools)
      OR bytearray_substring(l.topic2, 13, 20) IN (SELECT pool FROM pools))
),

-- last transfer INTO the pool before the Swap log = token sold by the taker
xfer_in AS (
    SELECT s.tx_hash, s.evt_index, x.token,
           row_number() OVER (PARTITION BY s.tx_hash, s.evt_index ORDER BY x.index DESC) AS rn
    FROM swaps s
    INNER JOIN transfers x ON x.tx_hash = s.tx_hash AND x."to" = s.pool AND x.index < s.evt_index
),

-- last transfer OUT of the pool before the Swap log = token bought by the taker
xfer_out AS (
    SELECT s.tx_hash, s.evt_index, x.token,
           row_number() OVER (PARTITION BY s.tx_hash, s.evt_index ORDER BY x.index DESC) AS rn
    FROM swaps s
    INNER JOIN transfers x ON x.tx_hash = s.tx_hash AND x."from" = s.pool AND x.index < s.evt_index
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
    o.token AS token_bought_address,
    i.token AS token_sold_address,
    s.recipient AS taker,
    CAST(NULL AS varbinary) AS maker,
    s.pool AS project_contract_address,
    s.tx_hash,
    s.evt_index
FROM swaps s
INNER JOIN xfer_in  i ON i.tx_hash = s.tx_hash AND i.evt_index = s.evt_index AND i.rn = 1
INNER JOIN xfer_out o ON o.tx_hash = s.tx_hash AND o.evt_index = s.evt_index AND o.rn = 1
WHERE s.delta0 <> 0 AND s.delta1 <> 0

{% endmacro %}
