{{ config(
    schema = 'curvefi',
    alias = alias('gauge_mappings'),
    post_hook='{{ expose_spells(\'["optimism"]\',
                                "project",
                                "curvefi",
                                \'["msilb7"]\') }}'
    )
}}

{% set curve_models = [
    ref('curvefi_optimism_gauge_mappings')
] %}


SELECT *
FROM (
    {% for gauge_model in curve_models %}
    SELECT
        blockchain
        , 'curve' as project
        , version
        , pool_contract
        , incentives_contract
        , incentives_type
        , evt_block_time
        , evt_block_number
        , contract_address
        , evt_tx_hash
        , evt_index
    FROM {{ gauge_model }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)
