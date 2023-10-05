{{ config(
        schema = 'tornado_cash_avalanche_c',
        alias = alias('deposits'),
        tags = ['dunesql'],
        materialized='incremental',
        partition_by=['block_date'],
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['block_date', 'tx_hash', 'evt_index'],
        post_hook='{{ expose_spells(\'["avalanche_c"]\',
                                    "project",
                                    "tornado_cash",
                                    \'["hildobby", "dot2dotseurat"]\') }}'
        )
}}

{% set avalanche_start_date = '2021-09-17' %}

SELECT tc.evt_block_time AS block_time
, 0xb31f66aa3c1e785363f0875a1b74e27b85fd66c7 AS currency_contract
, 'AVAX' AS currency_symbol
, 'avalanche_c' AS blockchain
, 'classic' AS tornado_version
, at."from" AS depositor
, tc.contract_address AS contract_address
, CASE WHEN tc.contract_address=0x330bdfade01ee9bf63c209ee33102dd334618e0a THEN 10
        WHEN tc.contract_address=0x1e34a77868e19a6647b1f2f47b51ed72dede95dd THEN 100
        WHEN tc.contract_address=0xaf8d1839c3c67cf571aa74b5c12398d4901147b3 THEN 500
        END AS amount
, tc.evt_tx_hash AS tx_hash
, tc.leafIndex AS leaf_index
, tc.evt_index
, TRY_CAST(date_trunc('DAY', tc.evt_block_time) AS date) AS block_date
FROM {{ source('tornado_cash_avalanche_c','ETHTornado_evt_Deposit') }} tc
INNER JOIN {{ source('avalanche_c','transactions') }} at
        ON at.hash=tc.evt_tx_hash
        {% if not is_incremental() %}
        AND at.block_time >= TIMESTAMP '{{avalanche_start_date}}'
        {% endif %}
        {% if is_incremental() %}
        AND at.block_time >= date_trunc('day', now() - interval '7' day)
        {% endif %}
{% if not is_incremental() %}
WHERE tc.evt_block_time >= TIMESTAMP '{{avalanche_start_date}}'
{% endif %}
{% if is_incremental() %}
WHERE tc.evt_block_time >= date_trunc('day', now() - interval '7' day)
{% endif %}