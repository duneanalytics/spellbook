{{ config(
    schema='sui_tvl',
    alias='tokens_detail',
    materialized='incremental',
    file_format='delta',
    incremental_strategy='merge',
    unique_key=['type_'],
    tags=['sui','tvl','tokens']
) }}

-- General coin metadata reference table
-- Converted from Snowflake dynamic table to dbt incremental model

with ranked_tokens as (
    select 
        cast(type_ as varchar) as type_,
        lower(regexp_extract(cast(type_ as varchar), '<(.*)>', 1)) as coin_type,
        cast(json_extract_scalar(object_json, '$.decimals') as integer) as coin_decimals,
        cast(json_extract_scalar(object_json, '$.name') as varchar) as coin_name,
        cast(json_extract_scalar(object_json, '$.symbol') as varchar) as coin_symbol,
        row_number() over (partition by cast(type_ as varchar) order by version desc) as rn
    from {{ source('sui','objects') }}
    where cast(type_ as varchar) like '0x2::coin::CoinMetadata%'
    {% if is_incremental() %}
    and checkpoint > coalesce((select max(checkpoint) from {{ this }}), 0)
    {% endif %}
)

select 
    type_,
    coin_type,
    coin_decimals,
    coin_name,
    coin_symbol
from ranked_tokens
where rn = 1 