{% set chains = gas_evm_chains() + ['tron'] %}

{{ config(
    schema = 'gas',
    alias = 'fees',
    post_hook='{{ expose_spells(blockchains = \'["' + chains | join('","') + '"]\',
                                spell_type = "sector",
                                spell_name = "gas",
                                contributors = \'["soispoke", "ilemi", "0xRob", "jeff-dude", "krishhh", "tomfutago"]\') }}'
        )
}}


SELECT
    *
FROM
(
    {% for blockchain in chains %}
    SELECT
        blockchain
        ,block_month
        ,block_date
        ,block_time
        ,block_number
        ,tx_hash
        ,tx_index
        ,tx_from
        ,tx_to
        ,gas_price
        ,gas_used
        ,currency_symbol
        ,tx_fee
        ,tx_fee_usd
        ,tx_fee_raw
        ,tx_fee_breakdown
        ,tx_fee_breakdown_usd
        ,tx_fee_breakdown_raw
        ,tx_fee_currency
        ,block_proposer
        ,gas_limit
        ,gas_limit_usage
    FROM
        {{ ref('gas_' ~ blockchain ~ '_fees') }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)
