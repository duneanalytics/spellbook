{{ config (
    
    alias = 'fees',
    post_hook = '{{ expose_spells(\'["ethereum"]\', "project", "tessera",\'["amadarrrr"]\') }}'
) }}
-- FEES GENERATED
WITH lpda_fees AS (
    SELECT
        _vault AS vault,
        _receiver AS receiver,
        'LPDA' AS source,
        CAST(_amount AS DOUBLE)/POWER(10, 18) AS amount,
        evt_block_time AS block_time,
        evt_tx_hash AS tx_hash
    FROM
        {{ source('tessera_ethereum','LPDA_evt_FeeDispersed') }}
)

SELECT *
FROM lpda_fees;
-- union with future sources
