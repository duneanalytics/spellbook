{{ config(
    tags=['dunesql'],
        alias = alias('lending'),
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['block_date', 'blockchain', 'project', 'version', 'tx_hash', 'evt_index', 'lien_id'],
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "sector",
                                    "nft",
                                    \'["Henrystats"]\') }}')
}}


{% set nft_models = [
 ref('bend_dao_ethereum_lending')
 ,ref('astaria_ethereum_lending')
] %}

SELECT *
FROM (
    {% for nft_model in nft_models %}
    SELECT
        blockchain,
        project,
        version,
        lending_category,
        block_date,
        block_month,
        block_time,
        block_number,
        lien_id,
        token_id,
        collection,
        amount_usd,
        token_standard,
        evt_type,
        borrower,
        lender,
        amount_original,
        amount_raw,
        collateral_currency_symbol,
        collateral_currency_contract,
        nft_contract_address,
        project_contract_address,
        tx_hash,
        tx_from,
        tx_to,
        evt_index
    FROM {{ nft_model }}
    {% if is_incremental() %}
    WHERE block_time >= date_trunc('day', now() - interval '7' Day)
    {% endif %}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}

) 