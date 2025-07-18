{{ config(
  materialized = 'table',
  schema = 'relay',
  alias = 'relay_token_transfers_all'
) }}

SELECT * FROM {{ ref('relay_token_transfers_ethereum') }}
UNION ALL
SELECT * FROM {{ ref('relay_token_transfers_optimism') }}
UNION ALL
SELECT * FROM {{ ref('relay_token_transfers_polygon') }}
UNION ALL
SELECT * FROM {{ ref('relay_token_transfers_abstract') }}
UNION ALL
SELECT * FROM {{ ref('relay_token_transfers_apechain') }}
UNION ALL
SELECT * FROM {{ ref('relay_token_transfers_arbitrum') }}
UNION ALL
SELECT * FROM {{ ref('relay_token_transfers_avalanche_c') }}
UNION ALL
SELECT * FROM {{ ref('relay_token_transfers_base') }}
UNION ALL
SELECT * FROM {{ ref('relay_token_transfers_nova') }}
UNION ALL
SELECT * FROM {{ ref('relay_token_transfers_b3') }}
UNION ALL
SELECT * FROM {{ ref('relay_token_transfers_berachain') }}
UNION ALL
SELECT * FROM {{ ref('relay_token_transfers_blast') }}
UNION ALL
SELECT * FROM {{ ref('relay_token_transfers_bnb') }}
UNION ALL
SELECT * FROM {{ ref('relay_token_transfers_bob') }}
UNION ALL
SELECT * FROM {{ ref('relay_token_transfers_boba') }}
UNION ALL
SELECT * FROM {{ ref('relay_token_transfers_celo') }}
UNION ALL
SELECT * FROM {{ ref('relay_token_transfers_corn') }}
UNION ALL
SELECT * FROM {{ ref('relay_token_transfers_degen') }}
UNION ALL
SELECT * FROM {{ ref('relay_token_transfers_fantom') }}
UNION ALL
SELECT * FROM {{ ref('relay_token_transfers_flare') }}
UNION ALL
SELECT * FROM {{ ref('relay_token_transfers_gnosis') }}
UNION ALL
SELECT * FROM {{ ref('relay_token_transfers_ink') }}
UNION ALL
SELECT * FROM {{ ref('relay_token_transfers_kaia') }}
UNION ALL
SELECT * FROM {{ ref('relay_token_transfers_lens') }}
UNION ALL
SELECT * FROM {{ ref('relay_token_transfers_linea') }}
UNION ALL
SELECT * FROM {{ ref('relay_token_transfers_mantle') }}
UNION ALL
SELECT * FROM {{ ref('relay_token_transfers_opbnb') }}
UNION ALL
SELECT * FROM {{ ref('relay_token_transfers_plume') }}
UNION ALL
SELECT * FROM {{ ref('relay_token_transfers_zkevm') }}
UNION ALL
SELECT * FROM {{ ref('relay_token_transfers_ronin') }}
UNION ALL
SELECT * FROM {{ ref('relay_token_transfers_scroll') }}
UNION ALL
SELECT * FROM {{ ref('relay_token_transfers_sei') }}
UNION ALL
SELECT * FROM {{ ref('relay_token_transfers_shape') }}
UNION ALL
SELECT * FROM {{ ref('relay_token_transfers_sonic') }}
UNION ALL
SELECT * FROM {{ ref('relay_token_transfers_sophon') }}
UNION ALL
SELECT * FROM {{ ref('relay_token_transfers_unichain') }}
UNION ALL
SELECT * FROM {{ ref('relay_token_transfers_viction') }}
UNION ALL
SELECT * FROM {{ ref('relay_token_transfers_worldchain') }}
UNION ALL
SELECT * FROM {{ ref('relay_token_transfers_zksync') }}
UNION ALL
SELECT * FROM {{ ref('relay_token_transfers_zora') }}
