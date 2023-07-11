{{ config(
        schema = 'tornado_cash_polygon',
        alias ='withdrawals',
        materialized='incremental',
        partition_by=['block_date'],
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['block_date', 'tx_hash', 'evt_index'],
        post_hook='{{ expose_spells(\'["polygon"]\',
                                    "project",
                                    "tornado_cash",
                                    \'["hildobby"]\') }}'
        )
}}

{% set polygon_start_date = '2021-06-28' %}

SELECT tc.evt_block_time AS block_time
, '0x0000000000000000000000000000000000001010' AS currency_contract
, 'MATIC' AS currency_symbol
, 'polygon' AS blockchain
, 'classic' AS tornado_version
, pt.from AS tx_from
, tc.nullifierHash AS nullifier
, tc.fee/POWER(10, 18) AS fee
, tc.relayer
, tc.to AS recipient
, tc.contract_address AS contract_address
, CASE WHEN tc.contract_address='0x1e34a77868e19a6647b1f2f47b51ed72dede95dd' THEN 0.1
        WHEN tc.contract_address='0xdf231d99ff8b6c6cbf4e9b9a945cbacef9339178' THEN 1
        WHEN tc.contract_address='0xaf4c0b70b2ea9fb7487c7cbb37ada259579fe040' THEN 10
        WHEN tc.contract_address='0xa5c2254e4253490c54cef0a4347fddb8f75a4998' THEN 100
        END AS amount
, tc.evt_tx_hash AS tx_hash
, tc.evt_index
, TRY_CAST(date_trunc('DAY', tc.evt_block_time) AS date) AS block_date
FROM {{ source('tornado_cash_polygon','TornadoCashMatic_evt_Withdrawal') }} tc
INNER JOIN {{ source('polygon','transactions') }} pt
        ON pt.hash=tc.evt_tx_hash
        {% if not is_incremental() %}
        AND pt.block_time >= '{{polygon_start_date}}'
        {% endif %}
        {% if is_incremental() %}
        AND pt.block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
{% if not is_incremental() %}
WHERE tc.evt_block_time >= '{{polygon_start_date}}'
{% endif %}
{% if is_incremental() %}
WHERE tc.evt_block_time >= date_trunc("day", now() - interval '1 week')
{% endif %}
