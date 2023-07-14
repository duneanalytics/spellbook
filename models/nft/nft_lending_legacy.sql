{{ config(
	tags=['legacy'],
	
        alias = alias('lending', legacy_model=True),
        partition_by = ['block_date'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['block_date', 'blockchain', 'project', 'version', 'tx_hash', 'evt_index'],
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "sector",
                                    "nft",
                                    \'["Henrystats"]\') }}')
}}


{% set nft_models = [
 ref('bend_dao_ethereum_lending_legacy')
] %}

SELECT *
FROM (
    {% for nft_model in nft_models %}
    SELECT
        blockchain,
        project,
        version,
        block_date,
        block_time,
        block_number,
        token_id,
        collection,
        amount_usd,
        token_standard,
        evt_type,
        address,
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
    WHERE block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}

)