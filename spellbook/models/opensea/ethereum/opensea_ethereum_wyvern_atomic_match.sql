 {{
  config(
    alias='wyvern_atomic_match'
  )
}}

SELECT
    DISTINCT call_tx_hash AS tx_hash,
    CASE
    WHEN addrs[6] = '0x0000000000000000000000000000000000000000' THEN '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
    ELSE addrs[6]
    END AS token_address
FROM
    {{ source('opensea_ethereum','wyvernexchange_call_atomicmatch_') }}
WHERE
    (
    addrs[3] = '0x5b3256965e7c3cf26e11fcaf296dfc8807c01073'
    OR addrs [10] = '0x5b3256965e7c3cf26e11fcaf296dfc8807c01073'
    )
    AND call_success

