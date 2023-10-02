-- PROTOFORM DISTRIBUTION BID. for example LPDA
{{ config (
    tags=['dunesql'],
    alias = alias('bids'),
    post_hook = '{{ expose_spells(\'["ethereum"]\', "project", "tessera",\'["amadarrrr"]\') }}'
) }}

WITH lpda_bid AS (
    SELECT
        _user AS user,
        _vault AS vault,
        'LPDA' AS type,
        CAST(_price AS DOUBLE) /POWER(10,18) AS price,
        _quantity AS amount,
        CAST(_price AS DOUBLE) /POWER(10,18) * CAST(_quantity AS DOUBLE) AS volume,
        evt_block_time AS block_time,
        evt_tx_hash AS tx_hash
    FROM
        {{ source('tessera_ethereum','LPDA_evt_BidEntered') }}
)

SELECT *
FROM lpda_bid;
-- union with future distribution modules
