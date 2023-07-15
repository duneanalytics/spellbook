{{config(
	tags=['legacy'],
	
    alias = alias('l2_fee_vaults', legacy_model=True),
    post_hook='{{ expose_spells(\'["optimism"]\',
                                "sector",
                                "labels",
                                \'["msilb7"]\') }}'
)}}

SELECT
  'optimism' as blockchain, address, vault_name AS name, 'infrastructure' AS category
, 'msilb7' as contributor, 'static' as source, timestamp('2023-06-02') as created_at
, now() AS updated_at, 'l2_fee_vaults' as model_name, 'identifier' as label_type

FROM {{ ref('addresses_optimism_fee_vaults_legacy') }}
