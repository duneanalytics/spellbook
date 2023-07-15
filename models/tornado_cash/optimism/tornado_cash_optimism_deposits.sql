{{ config(
        schema = 'tornado_cash_optimism',
        alias = alias('deposits'),
        materialized='incremental',
        partition_by=['block_date'],
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['block_date', 'tx_hash', 'evt_index'],
        post_hook='{{ expose_spells(\'["optimism"]\',
                                    "project",
                                    "tornado_cash",
                                    \'["hildobby", "dot2dotseurat"]\') }}'
        )
}}

{% set optimism_start_date = '2021-12-14' %}

SELECT tc.evt_block_time AS block_time
, '0x4200000000000000000000000000000000000006' AS currency_contract
, 'ETH' AS currency_symbol
, 'optimism' AS blockchain
, 'classic' AS tornado_version
, ot.from AS depositor
, tc.contract_address AS contract_address
, CASE WHEN tc.contract_address='0x84443cfd09a48af6ef360c6976c5392ac5023a1f' THEN 0.1
        WHEN tc.contract_address='0xb529a618ffedefe5cfa4df8dbb0b32d8c29ca2e8' THEN 0.1
        WHEN tc.contract_address='0xd47438c816c9e7f2e2888e060936a499af9582b3' THEN 1
        WHEN tc.contract_address='0x330bdfade01ee9bf63c209ee33102dd334618e0a' THEN 10
        WHEN tc.contract_address='0x1e34a77868e19a6647b1f2f47b51ed72dede95dd' THEN 100
        END AS amount
, tc.evt_tx_hash AS tx_hash
, tc.leafIndex AS leaf_index
, tc.evt_index
, TRY_CAST(date_trunc('DAY', tc.evt_block_time) AS date) AS block_date
FROM {{ source('tornado_cash_optimism','ETHTornado_evt_Deposit') }} tc
INNER JOIN {{ source('optimism','transactions') }} ot
        ON ot.hash=tc.evt_tx_hash
        {% if not is_incremental() %}
        AND ot.block_time >= '{{optimism_start_date}}'
        {% endif %}
        {% if is_incremental() %}
        AND ot.block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
{% if not is_incremental() %}
WHERE tc.evt_block_time >= '{{optimism_start_date}}'
{% endif %}
{% if is_incremental() %}
WHERE tc.evt_block_time >= date_trunc("day", now() - interval '1 week')
{% endif %}