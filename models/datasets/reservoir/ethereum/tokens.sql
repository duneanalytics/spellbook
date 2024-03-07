{{ config(    
    schema = 'reservoir',
    alias = 'tokens',
    post_hook = '{{ expose_dataset(\'["ethereum"]\',
                \'[""]\') }}'
    )
}}

WITH ranked_entries AS (
  SELECT
    created_at,
    collection_id,
    contract,
    floor_ask_maker,
    floor_ask_id,
    owner,
    description,
    floor_ask_source,
    floor_ask_valid_from,
    floor_ask_valid_to,
    floor_ask_value,
    id,
    last_sale_timestamp,
    last_sale_value,
    name,
    token_id,
    updated_at,
    ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC, filename DESC) AS row_num
  FROM
    {{ source('reservoir', 'tokens_0010') }}
)

SELECT
    t.created_at,
    t.collection_id,
    from_hex(t.contract) as contract,
    from_hex(t.floor_ask_maker) as floor_ask_maker,
    from_hex(t.floor_ask_id) as floor_ask_id,
    from_hex(t.owner) as owner,
    t.description,
    t.floor_ask_source,
    t.floor_ask_valid_from,
    t.floor_ask_valid_to,
    t.floor_ask_value,
    t.floor_ask_value / 1e18 as floor_ask_value_decimal,
    t.id,
    t.last_sale_timestamp,
    t.last_sale_value,
    t.last_sale_value / 1e18 as last_sale_value_decimal,
    t.name,
    t.token_id,
    t.updated_at
FROM
  ranked_entries t
WHERE
  row_num = 1
