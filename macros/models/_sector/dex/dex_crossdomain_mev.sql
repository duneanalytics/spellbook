{% macro dex_crossdomain_mev(blockchain, transactions) %}

WITH invalid_pubkeys AS (
    SELECT pubkey
    FROM (VALUES
        (0xb01dd8e44a8e02e36e0d66161103b9ff32315dbb9ae8c8ac8d097ba86a9e2b1eb3c7fd41e7cd1f77a987985639c26f52)
        , (0xac424d8a3e6ce38eb22109125357324a1c44ecad7a330a3d3deff91e68f4b567ba38c065d2cf852ef050d21705e5dfcb)
        , (0x918f080ca717afed4966901794ad8222ca618b523bbd3ce94be4a1240aa69d9be20f884950214a3cafa0404ce41213e1)
        , (0xa8bcbf91bff7d3368ddbf5b35c46a4f5d82b16230c851a4b8eec82be45225d339414170e14a6cd17ad83ee3792dead85)
        , (0x86f473a006c566f1648a82c74cdfbd4a3cb2ea04eb2e0d49ef381ab2562576888554ef3d39e56996f24c804abb489600)
        , (0x8c69edd7a8e8da5330787952a1ad5075516e6fd4bda1586d62dd64701f7628d5229eb7f929017dea9ae6995f9c69ef5e)
        , (0x80a29e569e8ced0be1fff42c845a59449aecf8a2503542e4e76763ccc0265e683e2d5d46618cc829349293ed08ff49ff)
        ) AS temp_table (pubkey)
    )

SELECT distinct s1.blockchain
, s1.project
, s1.version
, s1.block_time
, CAST(date_trunc('month', s1.block_time) AS date) AS block_month
, tx1.block_number
, s1.token_sold_address
, s1.token_bought_address
, s1.token_sold_symbol
, s1.token_bought_symbol
, s1.maker
, s1.taker
, s1.tx_hash
, s1.tx_from
, s1.tx_to
, s1.project_contract_address
, s1.token_pair
, tx1.index
, s1.token_sold_amount_raw
, s1.token_bought_amount_raw
, s1.token_sold_amount
, s1.token_bought_amount
, s1.amount_usd
, s1.evt_index
--, CASE WHEN s1.tx_from=s2.tx_from THEN 'tx_from' ELSE 'taker' END AS commonality
--, CASE WHEN s1.token_bought_address=s2.token_sold_address THEN 'token_sold' ELSE 'token_bought' END AS sandwiched_token
--, CASE WHEN s1.token_bought_address<s1.token_sold_address THEN 0 ELSE 1 END AS token_order
FROM {{ ref('dex_trades') }} s1
INNER JOIN {{ ref('dex_trades') }} s2 ON s1.blockchain='{{blockchain}}'
    AND s2.blockchain='{{blockchain}}'
    AND s1.block_time=s2.block_time
    AND s1.project=s2.project
    AND s1.version=s2.version
    AND s1.tx_hash!=s2.tx_hash
    AND s1.project_contract_address=s2.project_contract_address
    {% if is_incremental() %}
    AND s1.block_time >= date_trunc('day', now() - interval '7' day)
    AND s2.block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
INNER JOIN {{transactions}} tx1 ON tx1.block_time=s1.block_time
    AND tx1.hash=s1.tx_hash
    {% if is_incremental() %}
    AND tx1.block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
INNER JOIN {{transactions}} tx2 ON tx2.block_time=s2.block_time
    AND tx2.hash=s2.tx_hash
    AND (s1.tx_from=s2.tx_from OR s1.taker=s2.taker)
    AND ((tx1.index>tx2.index AND s1.token_bought_address=s2.token_sold_address)
        OR (tx1.index<tx2.index AND s1.token_sold_address=s2.token_bought_address))
    {% if is_incremental() %}
    AND tx2.block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}

{% endmacro %}