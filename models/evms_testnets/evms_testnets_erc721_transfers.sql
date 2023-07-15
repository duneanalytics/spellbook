{{ config(
        tags = ['dunesql'],
        alias = alias('erc721_transfers'),
        unique_key=['blockchain', 'tx_hash', 'evt_index'],
        post_hook='{{ expose_spells(\'["goerli"]\',
                                    "sector",
                                    "evms",
                                    \'["hildobby","msilb7"]\') }}'
        )
}}

{% set erc721_transfers_models = [
     ('goerli', source('erc721_goerli', 'evt_transfer'))

] %}

SELECT *
FROM (
        {% for erc721_transfers_model in erc721_transfers_models %}
        SELECT
        '{{ erc721_transfers_model[0] }}' AS blockchain
        , contract_address
        , evt_tx_hash
        , evt_index
        , evt_block_time
        , evt_block_number
        , "from"
        , to
        , tokenId
        FROM {{ erc721_transfers_model[1] }}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
        {% endfor %}
        );