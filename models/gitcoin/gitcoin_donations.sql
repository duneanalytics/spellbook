{{ config(
        
        alias = 'donations',
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['blockchain', 'tx_hash', 'evt_index'],
        post_hook='{{ expose_spells(\'["ethereum","polygon"]\',
                                    "sector",
                                    "donation",
                                    \'["hildobby"]\') }}')
}}


{% set gitcoin_models = [
ref('gitcoin_ethereum_donations')
, ref('gitcoin_polygon_donations')
] %}

SELECT *
FROM (
    {% for gitcoin_model in gitcoin_models %}
    SELECT
        blockchain,
        project,
        version,
        grant_round,
        block_date,
        block_month,
        block_time,
        block_number,
        amount_raw,
        amount_original,
        donor,
        recipient,
        currency_contract,
        currency_symbol,
        evt_index,
        contract_address,
        tx_hash
    FROM {{ gitcoin_model }}

    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}

)
