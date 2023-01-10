{{ config(
        alias ='accounting_liquidation',
        materialized = 'incremental',
        partition_by = ['code'],
        file_format = 'delta',
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                "project",
                                "maker",
                                \'["lyt", "adcv", "SebVentures", "steakhouse"]\') }}'
        )
}}
WITH liquidation_revenues AS (
    SELECT call_block_time           ts
         , call_tx_hash              hash
         , SUM(rad / POW(10, 45)) AS value
    FROM {{ source('maker_ethereum', 'vat_call_move') }}
    WHERE dst = '0xa950524441892a31ebddf91d3ceefa04bf454466'                -- vow
      AND call_success
      AND src NOT IN (SELECT contract_address FROM contracts)               -- contract_type = 'PSM' should be enough but letting it wider
      AND src NOT IN ('0xa13c0c8eb109f5a13c6c90fc26afb23beb3fb04a'
        , '0x621fe4fde2617ea8ffade08d0ff5a862ad287ec2')                     --aave v2 d3m, compuond v2 d3m
      AND call_tx_hash NOT IN (SELECT tx_hash FROM liquidation_excluded_tx) -- Exclude Flop income (coming directly from users wallets)
      AND call_tx_hash NOT IN (SELECT call_tx_hash FROM team_dai_burns_tx)
      AND call_tx_hash NOT IN (SELECT call_tx_hash FROM psm_yield_trxns)
      {% if is_incremental() %}
      AND call_block_time >= date_trunc("day", now() - interval '1 week')
      {% endif %}
    GROUP BY ts, hash
)
, liquidation_expenses AS (
    SELECT call_block_time           ts
         , call_tx_hash              hash
         , SUM(tab / POW(10, 45)) AS value
    FROM {{ source('maker_ethereum', 'vow_call_fess') }}
    WHERE call_success
      {% if is_incremental() %}
      AND call_block_time >= date_trunc("day", now() - interval '1 week')
      {% endif %}
    GROUP BY ts, hash
)
, liquidation AS (
    SELECT ts, hash, 31210 AS code, value AS value
    FROM liquidation_revenues --increased equity
    UNION ALL
    SELECT ts, hash, 21120 AS code, -value AS value
    FROM liquidation_revenues --reduced liability
    UNION ALL
    SELECT ts, hash, 31620 AS code, -value AS value
    FROM liquidation_expenses --decreased equity
    UNION ALL
    SELECT ts, hash, 21120 AS code, value AS value
    FROM liquidation_expenses --increased liability
)
SELECT ts,
       hash,
       code,
       value,
       'DAI'                           AS token,
       'Liquidation Revenues/Expenses' AS descriptor,
       NULL                            AS ilk
FROM liquidation
;