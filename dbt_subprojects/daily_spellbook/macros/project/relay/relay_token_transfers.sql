{% macro relay_token_transfers(blockchain) %}

WITH relay_tokens AS (
    SELECT * FROM {{ ref('relay_token_metadata') }} WHERE blockchain = '{{ blockchain }}'
),

excluded_addresses AS (
    SELECT * FROM (
        VALUES
    (0x000000000000000000000000000000000000800a),
    (0x0000000000000000000000000000000000008001),
    (0x0000000000000000000000000000000000000000),
    (0x000000000000000000000000000000000000dEaD),
    (0x0000000000000000000000000000000000000001),
    (0x0000000000000000000000000000000000000002),
    (0x0000000000000000000000000000000000000003),
    (0x0000000000000000000000000000000000001010),
    (0x000000000000000000000000000000000000dead),
    (0x0100000000000000000000000000000000000000),
            (0x000000000000000000000000000000000000800a),
            (0x0000000000000000000000000000000000008001),
            (0x0000000000000000000000000000000000000000)
    
    ) AS t(address)
),

txs AS (
    SELECT block_time, '{{ blockchain }}' AS blockchain, hash, block_number
    FROM {{ source(blockchain, 'transactions') }}
    WHERE "from" = 0xf70da97812CB96acDF810712Aa562db8dfA3dbEF
      AND success = TRUE
      AND value > 0
),

txs_erc20 AS (
    SELECT block_time, '{{ blockchain }}' AS blockchain, hash, block_number
    FROM {{ source(blockchain, 'transactions') }}
    WHERE "from" = 0xf70da97812CB96acDF810712Aa562db8dfA3dbEF
    AND success = TRUE
    AND value = 0
),


eth_transfers AS (
    SELECT
        DATE_TRUNC('day', e.block_time) AS day,
        '{{ blockchain }}' AS blockchain,
        e.hash AS evt_tx_hash,
        e.block_time AS evt_block_time,
        e."to",
        CASE
 WHEN '{{ blockchain }}' = 'bnb' THEN 'BNB'
WHEN '{{ blockchain }}' = 'gnosis' THEN 'xDAI'
WHEN '{{ blockchain }}' = 'polygon' THEN 'POL'
WHEN '{{ blockchain }}' = 'sonic' THEN 'S'
WHEN '{{ blockchain }}' = 'sei' THEN 'SEI'
WHEN '{{ blockchain }}' = 'ronin' THEN 'RON'
WHEN '{{ blockchain }}' = 'mantle' THEN 'MNT'
WHEN '{{ blockchain }}' = 'apechain' THEN 'APE'
WHEN '{{ blockchain }}' = 'celo' THEN 'CELO'
WHEN '{{ blockchain }}' = 'berachain' THEN 'BERA'
WHEN '{{ blockchain }}' = 'corn' THEN 'BTCN'
WHEN '{{ blockchain }}' = 'degen' THEN 'DEGEN'
    ELSE 'ETH'
        END AS symbol,
        e.value / 1e18 AS value,
        p.price,
        (e.value / 1e18) * p.price AS amount
    FROM {{ source(blockchain, 'transactions') }} e
    INNER JOIN txs tx
            ON e.hash = tx.hash
    LEFT JOIN {{ source('prices', 'day') }} p
        ON DATE_TRUNC('day', e.block_time) = p.timestamp
        AND p.blockchain = CASE
WHEN '{{ blockchain }}' = 'ronin' THEN 'ronin'
WHEN '{{ blockchain }}' = 'berachain' THEN 'berachain'
WHEN '{{ blockchain }}' = 'corn' THEN 'corn'
WHEN '{{ blockchain }}' = 'degen' THEN 'degen'
    ELSE 'ethereum'
        END
        AND p.contract_address = CASE
WHEN '{{ blockchain }}' = 'bnb' THEN 0xB8c77482e45F1F44dE1745F52C74426C631bDD52
WHEN '{{ blockchain }}' = 'gnosis' THEN 0x6b175474e89094c44da98b954eedeac495271d0f
WHEN '{{ blockchain }}' = 'polygon' THEN 0x455e53cbb86018ac2b8092fdcd39d8444affc3f6
WHEN '{{ blockchain }}' = 'sonic' THEN 0x4E15361FD6b4BB609Fa63C81A2be19d873717870
WHEN '{{ blockchain }}' = 'sei' THEN 0xbdF43ecAdC5ceF51B7D1772F722E40596BC1788B
WHEN '{{ blockchain }}' = 'ronin' THEN 0xe514d9deb7966c8be0ca922de8a064264ea6bcd4
WHEN '{{ blockchain }}' = 'mantle' THEN 0x3c3a81e81dc49a522a592e7622a7e711c06bf354
WHEN '{{ blockchain }}' = 'apechain' THEN 0x4d224452801aced8b2f0aebe155379bb5d594381
WHEN '{{ blockchain }}' = 'celo' THEN 0xE452E6Ea2dDeB012e20dB73bf5d3863A3Ac8d77a
WHEN '{{ blockchain }}' = 'berachain' THEN 0x6969696969696969696969696969696969696969
WHEN '{{ blockchain }}' = 'corn' THEN 0xda5dDd7270381A7C2717aD10D1c0ecB19e3CDFb2
WHEN '{{ blockchain }}' = 'degen' THEN 0xEb54dACB4C2ccb64F8074eceEa33b5eBb38E5387
    ELSE 0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2
        END
    WHERE e."from" = 0xf70da97812CB96acDF810712Aa562db8dfA3dbEF
      AND e."to" NOT IN (SELECT address FROM excluded_addresses)
),

erc20_transfers AS (
    SELECT DISTINCT
        day,
        blockchain,
        evt_tx_hash,
        evt_block_time,
        "to",
        symbol,
        value,
        price,
        amount
    FROM (
        SELECT
            DATE_TRUNC('day', e.evt_block_time) AS day,
            '{{ blockchain }}' AS blockchain,
            e.evt_tx_hash,
            e.evt_block_time,
            e."to",
            t.symbol,
            e.value / POWER(10, t.decimals) AS value,
            p.price,
            (e.value / POWER(10, t.decimals)) * p.price AS amount,
            ROW_NUMBER() OVER (PARTITION BY e.evt_tx_hash ORDER BY e.evt_index DESC) AS rn
        FROM {{ source('erc20_' ~ blockchain, 'evt_Transfer') }} e
        INNER JOIN relay_tokens t
            ON e.contract_address = t.contract_address AND '{{ blockchain }}' = t.blockchain
        INNER JOIN txs_erc20 tx
            ON e.evt_tx_hash = tx.hash
        LEFT JOIN {{ source('prices', 'day') }} p
            ON '{{ blockchain }}' = p.blockchain
            AND DATE_TRUNC('day', e.evt_block_time) = p.timestamp
            AND e.contract_address = p.contract_address
    ) with_ranks
    WHERE rn = 1
    AND "to" NOT IN (SELECT address FROM excluded_addresses)
)


SELECT DISTINCT * FROM (
SELECT * FROM erc20_transfers
UNION ALL
 SELECT * FROM eth_transfers
) combined_transfers

{% endmacro %}
