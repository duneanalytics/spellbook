{{ config(
    schema = 'toroperp',
    alias = 'deposits',
    post_hook='{{ expose_spells(blockchains = \'["sei"]\',
                                spell_type = "project",
                                spell_name = "toroperp",
                                contributors = \'["toroperp"]\') }}'
    )
}}

/*
    Toroperp broker deposits aggregated across all chains.
    Currently supports: SEI

    This model aggregates deposit data from all supported chains
    for the toroperp broker on Orderly Network.
*/

{% set toroperp_models = [
    ref('toroperp_sei_deposits')
] %}

SELECT *
FROM (
    {% for model in toroperp_models %}
    SELECT
        blockchain
        ,project
        ,version
        ,block_time
        ,block_date
        ,block_number
        ,tx_hash
        ,evt_index
        ,vault_contract
        ,account_id
        ,user_address
        ,deposit_nonce
        ,token_hash
        ,token_amount_raw
        ,tx_from
        ,tx_to
        ,broker_hash
    FROM {{ model }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)
