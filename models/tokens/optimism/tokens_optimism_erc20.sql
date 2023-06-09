{{ config( alias='erc20', materialized = 'table',
    post_hook='{{ expose_spells(\'["optimism"]\',
                                    "sector",
                                    "tokens",
                                    \'["msilb7"]\') }}')}}

SELECT *,
CASE WHEN token_type IN ('underlying') THEN 1
ELSE 0 --double counted (breakdown, receipt) or no price
END
AS is_counted_in_tvl
FROM (
	SELECT
	c.contract_address
	, coalesce(t.symbol,b.symbol) as symbol
	, coalesce(t.decimals,b.decimals) as decimals
	, coalesce(t.token_type,b.token_type) AS token_type
	, coalesce(t.token_mapping_source, b.token_mapping_source) AS token_mapping_source

	FROM {{ ref('tokens_optimism_erc20_transfer_source')}} c
	LEFT JOIN  {{ref('tokens_optimism_erc20_curated')}} t
	ON c.contract_address = t.contract_address
	LEFT JOIN {{ ref('tokens_optimism_erc20_generated')}} b
	ON c.contract_address = b.contract_address
	-- Eventually we can also try to map sectors here (i.e. stablecoin, liquid staking)
) a