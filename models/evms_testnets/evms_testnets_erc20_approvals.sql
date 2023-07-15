{{ config(
        tags = ['dunesql'],
        alias = alias('erc20_approvals'),
        unique_key=['blockchain', 'tx_hash', 'evt_index'],
        post_hook='{{ expose_spells(\'["goerli"]\',
                                    "sector",
                                    "evms",
                                    \'["hildobby","msilb7"]\') }}'
        )
}}

{% set erc20_approvals_models = [
     ('goerli', source('erc20_goerli', 'evt_approval'))

] %}

SELECT *
FROM (
        {% for erc20_approvals_model in erc20_approvals_models %}
        SELECT
        '{{ erc20_approvals_model[0] }}' AS blockchain
        , contract_address
        , evt_tx_hash
        , evt_index
        , evt_block_time
        , evt_block_number
        , owner
        , spender
        , value
        FROM {{ erc20_approvals_model[1] }}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
        {% endfor %}
        );