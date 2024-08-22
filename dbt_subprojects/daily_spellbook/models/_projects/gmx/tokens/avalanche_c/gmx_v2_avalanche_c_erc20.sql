{{
  config(
    schema = 'gmx_v2_avalanche_c',
    alias = 'erc20',
    materialized = 'table',
    post_hook='{{ expose_spells(\'["arbitrum"]\',
                                "project",
                                "gmx",
                                \'["ai_data_master","gmx-io"]\') }}'
  )
}}

{%- set tokens_url = 'https://avalanche-api.gmxinfra.io/tokens/dune' -%}

SELECT
    'avalanche_c' AS blockchain,
    'gmx-v2' AS project,
    json_extract_scalar(token, '$.symbol') AS symbol,
    from_hex(json_extract_scalar(token, '$.address')) AS contract_address,
    CAST(json_extract_scalar(token, '$.decimals') AS INTEGER) AS decimals,
    COALESCE(CAST(json_extract_scalar(token, '$.synthetic') AS BOOLEAN), false) AS synthetic,
    CURRENT_TIMESTAMP AS last_update_utc
FROM
    UNNEST(CAST(json_parse(http_get('{{tokens_url}}')) AS ARRAY(JSON))) AS t(token)