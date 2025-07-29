SELECT * FROM {{ ref('fungible_asset_metadata') }}
QUALIFY ROW_NUMBER() OVER (PARTITION BY asset_type ORDER BY block_time DESC) = 1
