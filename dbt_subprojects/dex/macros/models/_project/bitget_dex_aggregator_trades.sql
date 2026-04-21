{% macro bitget_dex_aggregator_trades(blockchain) %}

{% set project_start_date = '2025-10-01' %}

WITH paymaster_tx AS (
    SELECT DISTINCT
        tx_hash,
        block_date,
        MAX(CASE
            WHEN contract_address = 0xbc1d9760bd6ca468ca9fb5ff2cfbeac35d86c973 THEN '2'
            WHEN contract_address = 0xE17162B840cb9A8f6D9920E5832D58f6461caCe8 THEN '1'
        END) AS version
    FROM {{ source(blockchain, 'logs') }}
    WHERE block_date >= DATE('{{ project_start_date }}')
        {% if is_incremental() %}
        AND {{ incremental_predicate('block_time') }}
        {% endif %}
        AND contract_address IN (
            0xbc1d9760bd6ca468ca9fb5ff2cfbeac35d86c973,
            0xE17162B840cb9A8f6D9920E5832D58f6461caCe8
        )
        AND topic0 = 0x89a885b6900024aaed2c0845aad74f2204445bf00ac135917c70f57540e557b3
    GROUP BY tx_hash, block_date
)

SELECT
    trade.blockchain,
    'bitget_dex_aggregator' AS project,
    CASE
        WHEN paymaster_tx.version IS NOT NULL THEN paymaster_tx.version
        WHEN tx_to = 0xE17162B840cb9A8f6D9920E5832D58f6461caCe8 THEN '1'
        ELSE '2'
    END AS version,
    block_month,
    block_date,
    block_time,
    block_number,
    token_bought_symbol,
    token_sold_symbol,
    token_pair,
    token_bought_amount,
    token_sold_amount,
    token_bought_amount_raw,
    token_sold_amount_raw,
    amount_usd,
    token_bought_address,
    token_sold_address,
    taker,
    maker,
    project_contract_address,
    trade.tx_hash,
    tx_from,
    tx_to,
    CAST(ARRAY[-1] AS ARRAY<BIGINT>) AS trace_address,
    evt_index
FROM {{ ref('dex_' ~ blockchain ~ '_trades') }} trade
LEFT JOIN paymaster_tx
    ON trade.tx_hash = paymaster_tx.tx_hash
    AND trade.block_date = paymaster_tx.block_date
WHERE trade.block_date >= DATE('{{ project_start_date }}')
    {% if is_incremental() %}
    AND {{ incremental_predicate('trade.block_time') }}
    {% endif %}
    AND (
        tx_to IN (
            0xE17162B840cb9A8f6D9920E5832D58f6461caCe8,
            0xBc1D9760bd6ca468CA9fB5Ff2CFbEAC35d86c973,
            0x6752b178E2Ed13BCeE6951cEF907B44C95c5D630,
            0x704dE6944dE10b69a5357B9cB976Dbe89d6eA414
        )
        OR paymaster_tx.tx_hash IS NOT NULL
    )

{% endmacro %}
