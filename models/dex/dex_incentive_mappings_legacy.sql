{{ config(
	tags=['legacy'],
	
        alias = alias('incentive_mappings', legacy_model=True),
        post_hook='{{ expose_spells(\'["optimism"]\',
                                "sector",
                                "dex",
                                \'["msilb7"]\') }}'
        )
}}


{% set dex_inc_models = [
         ref('balancer_gauge_mappings_legacy')
        ,ref('velodrome_optimism_bribe_mappings_legacy')
] %}


SELECT *
FROM (
    {% for inc_model in dex_inc_models %}
    SELECT

        blockchain,
        project,
        version,
        pool_contract,
        incentives_contract,
        incentives_type,
        evt_block_time,
        evt_block_number,
        contract_address, 
        evt_tx_hash,
        evt_index

    FROM {{ inc_model }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)
