{{ config(
  materialized='table',
  schema='bridge_user_tracking',
  alias='pre_post_bridge_all'
) }}

SELECT * FROM {{ ref('pre_post_bridge_abstract') }}
UNION ALL
SELECT * FROM {{ ref('pre_post_bridge_apechain') }}
UNION ALL
SELECT * FROM {{ ref('pre_post_bridge_arbitrum') }}
UNION ALL
SELECT * FROM {{ ref('pre_post_bridge_nova') }}
UNION ALL
SELECT * FROM {{ ref('pre_post_bridge_avalanche_c') }}
UNION ALL
SELECT * FROM {{ ref('pre_post_bridge_b3') }}
UNION ALL
SELECT * FROM {{ ref('pre_post_bridge_base') }}
UNION ALL
SELECT * FROM {{ ref('pre_post_bridge_berachain') }}
UNION ALL
SELECT * FROM {{ ref('pre_post_bridge_blast') }}
UNION ALL
SELECT * FROM {{ ref('pre_post_bridge_bnb') }}
UNION ALL
SELECT * FROM {{ ref('pre_post_bridge_bob') }}
UNION ALL
SELECT * FROM {{ ref('pre_post_bridge_boba') }}
UNION ALL
SELECT * FROM {{ ref('pre_post_bridge_celo') }}
UNION ALL
SELECT * FROM {{ ref('pre_post_bridge_corn') }}
UNION ALL
SELECT * FROM {{ ref('pre_post_bridge_degen') }}
UNION ALL
SELECT * FROM {{ ref('pre_post_bridge_ethereum') }}
UNION ALL
SELECT * FROM {{ ref('pre_post_bridge_fantom') }}
UNION ALL
SELECT * FROM {{ ref('pre_post_bridge_flare') }}
UNION ALL
SELECT * FROM {{ ref('pre_post_bridge_gnosis') }}
UNION ALL
SELECT * FROM {{ ref('pre_post_bridge_ink') }}
UNION ALL
SELECT * FROM {{ ref('pre_post_bridge_kaia') }}
UNION ALL
SELECT * FROM {{ ref('pre_post_bridge_lens') }}
UNION ALL
SELECT * FROM {{ ref('pre_post_bridge_linea') }}
UNION ALL
SELECT * FROM {{ ref('pre_post_bridge_mantle') }}
UNION ALL
SELECT * FROM {{ ref('pre_post_bridge_mode') }}
UNION ALL
SELECT * FROM {{ ref('pre_post_bridge_opbnb') }}
UNION ALL
SELECT * FROM {{ ref('pre_post_bridge_optimism') }}
UNION ALL
SELECT * FROM {{ ref('pre_post_bridge_polygon') }}
UNION ALL
SELECT * FROM {{ ref('pre_post_bridge_zkevm') }}
UNION ALL
SELECT * FROM {{ ref('pre_post_bridge_ronin') }}
UNION ALL
SELECT * FROM {{ ref('pre_post_bridge_scroll') }}
UNION ALL
SELECT * FROM {{ ref('pre_post_bridge_sei') }}
UNION ALL
SELECT * FROM {{ ref('pre_post_bridge_shape') }}
UNION ALL
SELECT * FROM {{ ref('pre_post_bridge_sonic') }}
UNION ALL
SELECT * FROM {{ ref('pre_post_bridge_sophon') }}
UNION ALL
SELECT * FROM {{ ref('pre_post_bridge_unichain') }}
UNION ALL
SELECT * FROM {{ ref('pre_post_bridge_viction') }}
UNION ALL
SELECT * FROM {{ ref('pre_post_bridge_worldchain') }}
UNION ALL
SELECT * FROM {{ ref('pre_post_bridge_zksync') }}
UNION ALL
SELECT * FROM {{ ref('pre_post_bridge_zora') }}
