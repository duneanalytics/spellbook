{% macro lending_pike_compatible_supply(blockchain, project, version, evt_transfer_table) %}

WITH deployed_markets AS (
  SELECT
    output_pToken AS ptoken_address,
    JSON_EXTRACT_SCALAR(setupParams, '$.underlying') AS token_address,
    JSON_EXTRACT_SCALAR(setupParams, '$.symbol') AS symbol
  FROM {{ source(project ~ '_' ~ blockchain, 'factory_call_deploymarket') }}
),

    /* First, find all transactions where pTokens are minted */  
    supply_txs AS (
       SELECT DISTINCT
         evt_tx_hash,
         contract_address AS ptoken_address
       FROM {{ source(project ~ '_' ~ blockchain, evt_transfer_table) }}
       WHERE
         "from" = 0x0000000000000000000000000000000000000000
         AND contract_address IN (
           SELECT
             ptoken_address
           FROM deployed_markets
         )
     ),

    /* Get all the pToken mints that happened in these supply transactions */ 
    mint_details AS (
      SELECT
        t.evt_block_time,
        t.evt_block_number,
        t.evt_tx_hash,
        t.evt_index,
        t.contract_address AS ptoken_address,
        t."to" AS receiver,
        t.value AS ptoken_amount,
        dm.token_address as token_address,
        dm.symbol
      FROM {{ source(project ~ '_' ~ blockchain, evt_transfer_table) }} t
      JOIN supply_txs st
        ON t.evt_tx_hash = st.evt_tx_hash AND t.contract_address = st.ptoken_address
      JOIN deployed_markets dm
        ON t.contract_address = dm.ptoken_address
      WHERE
        t."from" = 0x0000000000000000000000000000000000000000
    ),

    /* Get all the underlying token transfers to the pToken contract in these transactions */ 
    token_transfers AS (
      SELECT
        md.evt_tx_hash,
        md.ptoken_address,
        t."from" AS sender,
        SUM(t.value) AS amount
      FROM mint_details AS md
      JOIN {{source(project ~ '_' ~ blockchain, evt_transfer_table) }} t
        ON md.evt_tx_hash = t.evt_tx_hash
        AND t.contract_address = md.ptoken_address
      GROUP BY
        md.evt_tx_hash,
        md.ptoken_address,
        t."from"
    )

SELECT 
    '{{ blockchain }}' AS blockchain,
    '{{ project }}' AS project,
    '{{ version }}' AS version,
    'supply' AS transaction_type,
    md.token_address,
    tt.sender AS depositor,
    md.receiver AS on_behalf_of,
    CAST(NULL AS varbinary) AS withdrawn_to,
    CAST(NULL AS varbinary) AS liquidator,
    tt.amount,
    DATE_TRUNC('month', md.evt_block_time) AS block_month,
    md.evt_block_time AS block_time,
    md.evt_block_number AS block_number,
    md.ptoken_address AS project_contract_address,
    md.evt_tx_hash AS tx_hash,
    md.evt_index
FROM mint_details md
JOIN token_transfers tt 
ON md.evt_tx_hash = tt.evt_tx_hash 
AND md.ptoken_address = tt.ptoken_address
{% endmacro %}