{{ config(
        alias ='deposits',
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
        ,transaction_type
        ,vault_address
        ,depositor
        ,withdrawn_to
        ,liquidator
        ,token_address
        ,token_symbol
        ,token_amount
        ,deposit_token_address
        ,deposit_token_amount
        ,deposit_token_symbol
        ,amount_usd
    FROM {{ lending_model }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)


