{% macro addresses_first_funding(blockchain) %}

SELECT 
    i.address
    , i.blockchain
    , i.first_funded_by
    , i.first_funded_by_block_time AS first_funded_at
    , CASE WHEN i.is_smart_contract THEN true ELSE false END AS is_smart_contract
    , CASE WHEN i.is_smart_contract THEN false ELSE true END AS is_eoa
    , now() AS _updated_at
FROM {{ ref('addresses_' + blockchain + '_info') }} i
{% if is_incremental() %}
WHERE {{ incremental_predicate('i.last_seen') }}
{% endif %}

{% endmacro %}
