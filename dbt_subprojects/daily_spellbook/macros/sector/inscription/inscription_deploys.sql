{% macro inscription_deploys(all_inscriptions) %}

SELECT blockchain
, block_time
, block_month
, block_number
, tx_hash
, tx_from
, tx_to
, tx_index
, json_extract_scalar(full_inscription, '$.p') AS inscription_standard
, json_extract_scalar(full_inscription, '$.op') AS operation
, json_extract_scalar(full_inscription, '$.tick') AS inscription_symbol
, try_cast(json_extract_scalar(full_inscription, '$.max') AS UINT256) AS max_supply
, try_cast(json_extract_scalar(full_inscription, '$.lim') AS UINT256) AS mint_limit
, full_inscription
FROM {{all_inscriptions}}
WHERE json_extract_scalar(full_inscription, '$.op') = 'deploy'

{% endmacro %}