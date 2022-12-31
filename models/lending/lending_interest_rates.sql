{{ config(
        alias ='interest_rates',
        post_hook='{{ expose_spells(\'["ethereum", "optimism",]\',
                                "sector",
                                "lending",
                                \'["augustog", "Henrystats", "msilb7"]\') }}'
        )
}}


{% set lending_models = [
    -- TODO: add aave & ironbank after matching existing spells to this structure
] %}


SELECT *
FROM (
    {% for lending_model in lending_models %}
    SELECT
        blockchain
        ,project
        ,version
        ,block_date
        ,block_time
        ,tx_hash
        ,evt_index
        ,vault_address
        ,token_address
        ,token_symbol
        ,deposit_apy
        ,stable_borrow_apy
        ,variable_borrow_apy
    FROM {{ lending_model }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)


