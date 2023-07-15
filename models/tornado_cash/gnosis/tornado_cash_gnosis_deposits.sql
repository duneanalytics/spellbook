{{ config(
        schema = 'tornado_cash_gnosis',
        alias = alias('deposits'),
        materialized='incremental',
        partition_by=['block_date'],
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['block_date', 'tx_hash', 'evt_index'],
        post_hook='{{ expose_spells(\'["gnosis"]\',
                                    "project",
                                    "tornado_cash",
                                    \'["hildobby", "dot2dotseurat"]\') }}'
        )
}}

{% set gnosis_start_date = '2021-08-25' %}

SELECT tc.evt_block_time AS block_time
, '0xe91d153e0b41518a2ce8dd3d7944fa863463a97d' AS currency_contract
, 'xDAI' AS currency_symbol
, 'gnosis' AS blockchain
, 'classic' AS tornado_version
, gt.from AS depositor
, tc.contract_address AS contract_address
, CASE WHEN tc.contract_address='0x1e34a77868e19a6647b1f2f47b51ed72dede95dd' THEN 100
        WHEN tc.contract_address='0xdf231d99ff8b6c6cbf4e9b9a945cbacef9339178' THEN 1000
        WHEN tc.contract_address='0xaf4c0b70b2ea9fb7487c7cbb37ada259579fe040' THEN 10000
        WHEN tc.contract_address='0xa5c2254e4253490c54cef0a4347fddb8f75a4998' THEN 100000
        END AS amount
, tc.evt_tx_hash AS tx_hash
, tc.leafIndex AS leaf_index
, tc.evt_index
, TRY_CAST(date_trunc('DAY', tc.evt_block_time) AS date) AS block_date
FROM {{ source('tornado_cash_gnosis','eth_evt_Deposit') }} tc
INNER JOIN {{ source('gnosis','transactions') }} gt
        ON gt.hash=tc.evt_tx_hash
        {% if not is_incremental() %}
        AND gt.block_time >= '{{gnosis_start_date}}'
        {% endif %}
        {% if is_incremental() %}
        AND gt.block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
{% if not is_incremental() %}
WHERE tc.evt_block_time >= '{{gnosis_start_date}}'
{% endif %}
{% if is_incremental() %}
WHERE tc.evt_block_time >= date_trunc("day", now() - interval '1 week')
{% endif %}