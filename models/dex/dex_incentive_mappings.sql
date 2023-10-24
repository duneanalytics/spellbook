{{ config(
        alias = 'incentive_mappings',
        
        post_hook='{{ expose_spells(\'["optimism","base"]\',
                                "sector",
                                "dex",
                                \'["msilb7"]\') }}'
        )
}}


{% set dex_inc_models = [
         ref('balancer_gauge_mappings')
        ,ref('velodrome_optimism_bribe_mappings')
        ,ref('aerodrome_base_bribe_mappings')
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
