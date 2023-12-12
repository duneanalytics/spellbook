{% macro usage_summary_daily_logs( chain, start_date = '2015-01-01', end_date = '9999-12-31' ) %}


  SELECT
    '{{chain}}' as blockchain
  , block_date
  , DATE_TRUNC('month', block_date ) AS block_month
  , COALESCE(contract_address, 0x) AS address
  ---
  , COUNT(DISTINCT block_number) AS num_logs_emitted_blocks
  , COUNT(DISTINCT tx_hash) AS num_logs_emitted_txs
  , COUNT(*) AS num_logs_emitted_events
  ---
  , COUNT(DISTINCT tx_from) AS num_logs_emitted_tx_senders
  , SUM(CASE WHEN logs_emitted_number = 1 THEN tx_gas_used ELSE 0 END) AS sum_logs_emitted_tx_gas_used
  

  FROM (
      SELECT l.block_date, l.block_number, l.contract_address, l.tx_hash, l.tx_from
        , t.gas_used as tx_gas_used
        , ROW_NUMBER() OVER (PARTITION BY l.tx_hash) AS logs_emitted_number --reindex log to ensure single count

        FROM {{ source(chain,'logs') }} l
        INNER JOIN  {{ source(chain,'transactions') }} t 
          ON t.hash = l.tx_hash
          AND t.block_number = l.block_number
          AND t.block_date = l.block_date
          {% if is_incremental() %}
          AND t.block_date >= DATE_TRUNC('day', NOW() - interval '1' day) --ensure we capture whole days, with 1 day buffer depending on spell runtime
          -- AND [[ incremental_predicate('t.block_date') ]]
          {% endif %}
          AND t.block_date BETWEEN cast( '{{start_date}}' as date) AND cast( '{{end_date}}' as date)
        
        WHERE 1=1
          AND t.success
          {% if is_incremental() %}
          AND l.block_date >= DATE_TRUNC('day', NOW() - interval '1' day) --ensure we capture whole days, with 1 day buffer depending on spell runtime
          AND t.block_date >= DATE_TRUNC('day', NOW() - interval '1' day) --ensure we capture whole days, with 1 day buffer depending on spell runtime
          -- AND [[ incremental_predicate('l.block_date') ]]
          -- AND [[ incremental_predicate('t.block_date') ]]
          {% endif %}
          AND l.block_date BETWEEN cast( '{{start_date}}' as date) AND cast( '{{end_date}}' as date)
          AND t.block_date BETWEEN cast( '{{start_date}}' as date) AND cast( '{{end_date}}' as date)
      GROUP BY 1,2,3,4,5,6
    ) a
  GROUP BY 1,2,3,4

{% endmacro %}