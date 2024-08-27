{{
  config(
    schema = 'gmx_v2_arbitrum',
    alias = 'collateral_tokens_data',    
    materialized = 'view'
    )
}}

{%- set url_path = 'https://arbitrum-api.gmxinfra.io/tokens/dune' -%}
{%- set blockchain_name = 'arbitrum' -%}

SELECT
    '{{ blockchain_name }}' AS blockchain,
    'gmx_v2' AS project,
    json_extract_scalar(token, '$.symbol') AS symbol,
    from_hex(json_extract_scalar(token, '$.address')) AS contract_address,
    CAST(json_extract_scalar(token, '$.decimals') AS INTEGER) AS decimals,
    COALESCE(CAST(json_extract_scalar(token, '$.synthetic') AS BOOLEAN), false) AS synthetic,
    CURRENT_TIMESTAMP AS last_update_utc
FROM
    UNNEST(CAST(json_parse(http_get('{{ url_path }}')) AS ARRAY(JSON))) AS t(token)
