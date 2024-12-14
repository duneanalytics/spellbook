
{{
    config(
        schema = 'babylon_btc'
        ,alias = 'finality_providers'
        ,materialized = 'table'
        ,post_hook='{{ expose_spells(\'["bitcoin"]\',
                        "project",
                        "babylon",
                        \'["maybeYonas", "pyor_xyz"]\') }}'
    )
}}

select 
    json_extract_scalar(j, '$.active_delegations') as active_delegations,
    json_extract_scalar(j, '$.active_tvl') as active_tvl,
    json_extract_scalar(j, '$.btc_pk') as btc_pk,
    json_extract_scalar(j, '$.commission') as commission,
    -- json_extract_scalar(j, '$.description') as description,
    json_extract_scalar(j, '$.description.details') as details,
    json_extract_scalar(j, '$.description.identity') as identity,
    json_extract_scalar(j, '$.description.moniker') as moniker,
    json_extract_scalar(j, '$.description.security_contact') as security_contact,
    json_extract_scalar(j, '$.description.website') as website,
    json_extract_scalar(j, '$.total_delegations') as total_delegations,
    json_extract_scalar(j, '$.total_tvl') as total_tvl
from unnest(
    cast(
        json_extract(
            cast(json_parse(
                http_get('https://staking-api.babylonlabs.io/v1/finality-providers?pagination_key='))
            as json),
            '$.data'
        ) as array(json)
    )
    -- )
) as t(j)
