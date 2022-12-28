-- PROTOFORM DISTRIBUTION BID. for example LPDA
{{ config (
    alias = 'bids',
    post_hook = '{{ expose_spells(\'["ethereum"]\', "project", "tessera",\'["amadarrrr"]\') }}'
) }}

WITH lpda_bid AS (
    SELECT
        _user AS user,
        _vault AS vault,
        'LPDA' AS type,
        _price/POWER(10,18) AS price,
        _quantity AS amount,
        _price/POWER(10,18)*_quantity AS volume,
        evt_block_time AS block_time,
        evt_tx_hash AS tx_hash
    FROM
        {{ source('tessera_ethereum','LPDA_evt_BidEntered') }}
)

SELECT *
FROM lpda_bid;
-- union with future distribution modules
