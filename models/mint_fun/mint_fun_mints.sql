{{
    config(
        tags = ['dunesql'],
        alias = alias('mints'),
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['blockchain', 'tx_hash', 'evt_index'],
        post_hook='{{ expose_spells(\'["ethereum", "optimism", "base"]\',
                                "project",
                                "mint_fun",
                                \'["hildobby"]\') }}'
    )
}}

{% set mint_fun_models = [
    ('ethereum', ref('mint_fun_ethereum_mints'))
    , ('optimism', ref('mint_fun_optimism_mints'))
    , ('base', ref('mint_fun_base_mints'))
    
] %}

SELECT *
FROM (
        {% for mint_fun_model in mint_fun_models %}
        SELECT
        '{{ mint_fun_model[0] }}' AS blockchain
        , block_time
        , block_number
        , tx_from
        , recipient
        , nft_contract_address
        , nft_token_id
        , nft_amount
        , price_raw
        , price
        , currency_address
        , currency_symbol
        , tx_hash
        , evt_index
        FROM {{ mint_fun_model[1] }}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
        {% endfor %}
        );