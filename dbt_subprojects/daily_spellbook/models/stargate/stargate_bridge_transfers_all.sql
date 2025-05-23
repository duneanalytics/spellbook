{{ config(
  materialized='table',
  schema='stargate',
  alias='stargate_bridge_transfers_all'
) }}

SELECT * FROM {{ ref('stargate_bridge_transfers_abstract') }}
UNION ALL
SELECT * FROM {{ ref('stargate_bridge_transfers_apechain') }}
UNION ALL
SELECT * FROM {{ ref('stargate_bridge_transfers_arbitrum') }}
UNION ALL
SELECT * FROM {{ ref('stargate_bridge_transfers_nova') }}
UNION ALL
SELECT * FROM {{ ref('stargate_bridge_transfers_avalanche_c') }}
UNION ALL
SELECT * FROM {{ ref('stargate_bridge_transfers_b3') }}
UNION ALL
SELECT * FROM {{ ref('stargate_bridge_transfers_base') }}
UNION ALL
SELECT * FROM {{ ref('stargate_bridge_transfers_berachain') }}
UNION ALL
SELECT * FROM {{ ref('stargate_bridge_transfers_blast') }}
UNION ALL
SELECT * FROM {{ ref('stargate_bridge_transfers_bnb') }}
UNION ALL
SELECT * FROM {{ ref('stargate_bridge_transfers_bob') }}
UNION ALL
SELECT * FROM {{ ref('stargate_bridge_transfers_boba') }}
UNION ALL
SELECT * FROM {{ ref('stargate_bridge_transfers_celo') }}
UNION ALL
SELECT * FROM {{ ref('stargate_bridge_transfers_corn') }}
UNION ALL
SELECT * FROM {{ ref('stargate_bridge_transfers_degen') }}
UNION ALL
SELECT * FROM {{ ref('stargate_bridge_transfers_ethereum') }}
UNION ALL
SELECT * FROM {{ ref('stargate_bridge_transfers_fantom') }}
UNION ALL
SELECT * FROM {{ ref('stargate_bridge_transfers_flare') }}
UNION ALL
SELECT * FROM {{ ref('stargate_bridge_transfers_gnosis') }}
UNION ALL
SELECT * FROM {{ ref('stargate_bridge_transfers_ink') }}
UNION ALL
SELECT * FROM {{ ref('stargate_bridge_transfers_kaia') }}
UNION ALL
SELECT * FROM {{ ref('stargate_bridge_transfers_lens') }}
UNION ALL
SELECT * FROM {{ ref('stargate_bridge_transfers_linea') }}
UNION ALL
SELECT * FROM {{ ref('stargate_bridge_transfers_mantle') }}
UNION ALL
SELECT * FROM {{ ref('stargate_bridge_transfers_mode') }}
UNION ALL
SELECT * FROM {{ ref('stargate_bridge_transfers_opbnb') }}
UNION ALL
SELECT * FROM {{ ref('stargate_bridge_transfers_optimism') }}
UNION ALL
SELECT * FROM {{ ref('stargate_bridge_transfers_polygon') }}
UNION ALL
SELECT * FROM {{ ref('stargate_bridge_transfers_zkevm') }}
UNION ALL
SELECT * FROM {{ ref('stargate_bridge_transfers_ronin') }}
UNION ALL
SELECT * FROM {{ ref('stargate_bridge_transfers_scroll') }}
UNION ALL
SELECT * FROM {{ ref('stargate_bridge_transfers_sei') }}
UNION ALL
SELECT * FROM {{ ref('stargate_bridge_transfers_shape') }}
UNION ALL
SELECT * FROM {{ ref('stargate_bridge_transfers_sonic') }}
UNION ALL
SELECT * FROM {{ ref('stargate_bridge_transfers_sophon') }}
UNION ALL
SELECT * FROM {{ ref('stargate_bridge_transfers_unichain') }}
UNION ALL
SELECT * FROM {{ ref('stargate_bridge_transfers_viction') }}
UNION ALL
SELECT * FROM {{ ref('stargate_bridge_transfers_worldchain') }}
UNION ALL
SELECT * FROM {{ ref('stargate_bridge_transfers_zksync') }}
UNION ALL
SELECT * FROM {{ ref('stargate_bridge_transfers_zora') }}
