{{ config (
    alias = alias('distributions'),
    post_hook = '{{ expose_spells(\'["ethereum"]\', "project", "tessera",\'["amadarrrr"]\') }}'
) }}
-- PROTOFORM DISTRIBUTION. for example LPDA
WITH lpda_creations AS (
    SELECT
        _id AS id,
        _token AS token,
        _vault AS vault,
        'LPDA' AS type,
        _lpdaInfo AS info,
        evt_block_time AS block_time,
        evt_tx_hash AS tx_hash
    FROM
        {{ source('tessera_ethereum','LPDA_evt_CreatedLPDA') }}
)

SELECT *
FROM lpda_creations;
-- union with future distribution modules
