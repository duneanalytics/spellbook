{{ config(
        alias ='withdrawals',
        materialized='incremental',
        partition_by=['block_date'],
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['blockchain', 'block_date', 'tx_hash', 'evt_index'],
        post_hook='{{ expose_spells(\'["ethereum", "bnb", "avalanche_c", "gnosis", "optimism", "arbitrum", "polygon"]\',
                                    "project",
                                    "tornado_cash",
                                    \'["hildobby", "dot2dotseurat"]\') }}'
        )
}}


{% set tornado_cash_withdrawals_models = [
ref('tornado_cash_arbitrum_withdrawals')
,ref('tornado_cash_avalanche_c_withdrawals')
,ref('tornado_cash_bnb_withdrawals')
,ref('tornado_cash_ethereum_withdrawals')
,ref('tornado_cash_gnosis_withdrawals')
,ref('tornado_cash_optimism_withdrawals')
,ref('tornado_cash_polygon_withdrawals')
] %}

SELECT *
FROM (
    {% for tornado_cash_withdrawals_model in tornado_cash_withdrawals_models %}
    SELECT block_time
        , currency_contract
        , currency_symbol
        , blockchain
        , tornado_version
        , tx_from
        , nullifier
        , fee
        , relayer
        , recipient
        , contract_address
        , amount
        , tx_hash
        , evt_index
        , block_date
    FROM {{ tornado_cash_withdrawals_model }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}

)